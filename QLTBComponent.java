/*

Copyright 2013, 2014 solute GmbH.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

*/

package com.billiger.solr.handler.component;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.WeakHashMap;

import javax.xml.xpath.XPathConstants;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.solr.cloud.ZkController;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.Config;
import org.apache.solr.core.SolrCore;
import org.apache.solr.handler.component.ResponseBuilder;
import org.apache.solr.handler.component.SearchComponent;
import org.apache.solr.schema.FieldType;
import org.apache.solr.search.SolrIndexSearcher;
import org.apache.solr.util.DOMUtil;
import org.apache.solr.util.RefCounted;
import org.apache.solr.util.VersionedFile;
import org.apache.solr.util.plugin.SolrCoreAware;


/**
 * Query-Local Term Boost component.
 *
 * This component inserts boosted terms into the SOLR query depending
 * on the query string.
 *
 * Each such query string and its boost terms (or block terms) are listed
 * in one huge XML file.
 *
 * In the prepare() stage, the user's query is analyzed
 * and checked against the term boost map. If a matching entry
 * is found, the request is rewritten: the original query put inside a
 * new BooleanQuery as a MUST term, and the stored ConstScoreQueries are
 * added to this BooleanQuery as either SHOULD (boost &gt; 0) or MUST_NOT
 * (boost &lt;= 0) terms.
 *
 * The XML file describes what terms are to be boosted for what queries:
 * <pre>

<boosts>
  <query text="galaxy samsung tab">
    <term boost="60" field="cat_id" value="8842"/>
    <term boost="48000" field="id" value="1:380390654"/>
    <term boost="11000" field="id" value="1:409360524"/>
  </query>
</boosts>

 * </pre>
 * This file contains for each query a list of terms, each of which
 * is defined by a field and its value, plus a boost factor. From
 * this, a set of ConstantScoreQuery objects is created and stored
 * in a map under the analyzed query string. (A constant score is used
 * to account for possibly huge differences in terms' idf values, thus
 * boosts remain comparable: a boost of 10.0 is always better than a
 * boost of 8.0, regardless of the idf values, which could potentially
 * affect the absolute boost effectiveness.)
 *
 * The XML file with the queries and boost terms is cached: if it resides
 * in conf/ it is reloaded upon core reload (the QLTBComponent is
 * SolrCoreAware). if it resides in the data/
 * directory, it is reloaded every time a new IndexReader is opened. The
 * cache is flushed on core reload.
 *
 * The analyzer used to process the query strings, both from the user and
 * from the XML file, is configured by giving the component a fieldType
 * in solrconfig.xml via the queryFieldType element:

<searchComponent name="qltb" class="com.billiger.solr.handler.component.QLTBComponent">
  <str name="componentName">qltb</str>
  <str name="qltbFile">qltb.xml</str>
  <str name="queryFieldType">query_elevation</str>
</searchComponent>

 * The field type is configured in the schema.xml:

<fieldType name="query_elevation" class="solr.TextField">
  <analyzer type="query">
    <tokenizer class="solr.StandardTokenizerFactory" />
    <filter class="solr.LowerCaseFilterFactory" />
  </analyzer>
</fieldType>

 * The componentName (from the searchComponent element) can be used to
 * deactivate the component on a per-request basis, by passing a parameter
 * &lt;componentName&gt;=false to SOLR.
 *
 * @author Patrick Schemitz
 * @author Jan Morlock
 * @author Frank Honza
 * @author Information Retrieval Dept, solute GmbH
 * @author InformationRetrieval@solute.de
 *
 */
public class QLTBComponent extends SearchComponent implements SolrCoreAware {

    private static Logger log = LoggerFactory.getLogger(QLTBComponent.class);

    private static final String DISABLE_COMPONENT_PARAM = "opt_out";

    private static final String COMPONENT_NAME = "componentName";

    private static final String QLTB_FILE = "qltbFile";

    private static final String FIELD_TYPE = "queryFieldType";

    private SolrParams initArgs = null;

    private Analyzer analyzer = null;

    private final Map<IndexReader, Map<String, List<Query>>> qltbCache = new WeakHashMap<IndexReader, Map<String, List<Query>>>();

    @SuppressWarnings("rawtypes")
    @Override
    public void init(NamedList args) {
        this.initArgs = SolrParams.toSolrParams(args);
    }

    /**
     * Get analyzed version of the query string.
     *
     * This uses the analyzer for the configured FieldType for this
     * component to analyze and re-assemble the original query string.
     * If no queryFieldType is configured, the original query will be
     * returned.
     *
     * This is used both in the prepare() stage of the component and
     * when reading the QLTB map data.
     */
    String getAnalyzedQuery(String query) throws IOException {
        if (analyzer == null) {
            return query;
        }
        StringBuilder norm = new StringBuilder();
        TokenStream tokens = analyzer.tokenStream("", new StringReader(query));
        tokens.reset();
        CharTermAttribute termAtt = tokens .addAttribute(CharTermAttribute.class);
        while (tokens.incrementToken()) {
            norm.append(termAtt.buffer(), 0, termAtt.length());
        }
        tokens.end();
        tokens.close();
        return norm.toString();
    }

    /**
     * Inform component of core reload.
     *
     * This will both set the analyzer according to the configured
     * queryFieldType, and load the QLTB data. Data source can be (in this
     * order) ZooKeeper, the conf/ directory or the data/ directory.
     */
    @Override
    public final void inform(final SolrCore core) {
        // load analyzer
        String queryFieldType = initArgs.get(FIELD_TYPE);
        if (queryFieldType != null) {
            FieldType ft = core.getLatestSchema().getFieldTypes().get(queryFieldType);
            if (ft == null) {
                throw new SolrException(
                    SolrException.ErrorCode.SERVER_ERROR,
                    "unknown FieldType \"" + queryFieldType
                    + "\" used in QLTBComponent"
                );
            }
            analyzer = ft.getQueryAnalyzer();
        } else {
            analyzer = null;
        }
        synchronized (qltbCache) {
            qltbCache.clear();
            try {
                // retrieve QLTB data filename
                String qltbFile = initArgs.get(QLTB_FILE);
                if (qltbFile == null) {
                    throw new SolrException(
                        SolrException.ErrorCode.SERVER_ERROR,
                        "QLTBComponent must specify argument: \""
                        + QLTB_FILE + "\" - path to QLTB data"
                    );
                }
                boolean exists = false;
                // check ZooKeeper
                ZkController zkController = core.getCoreDescriptor().getCoreContainer().getZkController();
                if (zkController != null) {
                    exists = zkController.configFileExists(
                        zkController.readConfigName(
                            core.getCoreDescriptor().getCloudDescriptor().getCollectionName()
                        ),
                        qltbFile
                    );
                } else {
                    // no ZooKeeper, check conf/ and data/ directories
                    File fConf = new File(core.getResourceLoader().getConfigDir(), qltbFile);
                    File fData = new File(core.getDataDir(), qltbFile);
                    if (fConf.exists() == fData.exists()) {
                        // both or neither exist
                        throw new SolrException(
                            SolrException.ErrorCode.SERVER_ERROR,
                            "QLTBComponent missing config file: \"" + qltbFile
                            + "\": either " + fConf.getAbsolutePath() + " or "
                            + fData.getAbsolutePath() + " must exist, but not both"
                        );
                    }
                    if (fConf.exists()) {
                        // conf/ found, load it
                        exists = true;
                        log.info("QLTB source conf/: " + fConf.getAbsolutePath());
                        Config cfg = new Config(core.getResourceLoader(), qltbFile);
                        qltbCache.put(null, loadQLTBMap(cfg, core));
                    }
                }
                if (!exists) {
                    // Neither ZooKeeper nor conf/, so must be in data/
                    // We need an IndexReader and the normal
                    RefCounted<SolrIndexSearcher> searcher = null;
                    try {
                        searcher = core.getNewestSearcher(false);
                        IndexReader reader = searcher.get().getIndexReader();
                        getQLTBMap(reader, core);
                    } finally {
                        if (searcher != null) {
                            searcher.decref();
                        }
                    }
                }
            } catch (Exception ex) {
                throw new SolrException(
                    SolrException.ErrorCode.SERVER_ERROR,
                    "Error initializing QltbComponent.",
                    ex
                );
            }
        }
    }

    /**
     * Get QLTB map for the given IndexReader.
     *
     * If the QLTB map is located in the conf/ directory, it is independent
     * of the IndexReader and reloaded only during a core reload.
     * If, however, QLTB data is read from ZooKeeper or the data/ directory,
     * it is reloaded for each new IndexReader via the core's resource loader.
     *
     * @return QLTB map for the given IndexReader.
     */
    private Map<String, List<Query>> getQLTBMap(final IndexReader reader, final SolrCore core) throws Exception {
        Map<String, List<Query>> map = null;
        synchronized (qltbCache) {
            map = qltbCache.get(null); // Magic "null" key for data from conf/
            if (map != null) {
                // QLTB data from the conf/ directory, reader-independent.
                return map;
            }
            map = qltbCache.get(reader);
            if (map == null) {
                // No QLTB map for this reader yet, load it from ZooKeeper or
                // the data/ directory.
                log.info("load QLTB map for new IndexReader");
                String qltbFile = initArgs.get(QLTB_FILE);
                if (qltbFile == null) {
                    throw new SolrException(
                        SolrException.ErrorCode.SERVER_ERROR,
                        "QLTBComponent must specify argument: " + QLTB_FILE
                    );
                }
                Config cfg;
                ZkController zkController = core.getCoreDescriptor().getCoreContainer().getZkController();
                if (zkController != null) {
                    // We're running under ZooKeeper control...
                    cfg = new Config(core.getResourceLoader(), qltbFile, null, null);
                } else {
                    // No ZooKeeper, use data/ directory
                    InputStream is = VersionedFile.getLatestFile(core.getDataDir(), qltbFile);
                    cfg = new Config(core.getResourceLoader(), qltbFile, new InputSource(is), null);
                }
                map = loadQLTBMap(cfg, core);
                qltbCache.put(reader, map);
            }
            return map;
        }
    }

    /**
     * Load the QLTB map from a Config.
     *
     * Read and process the "boosts/query" XPath nodes from the given
     * Config, and build them into a QLTB map. The XML format is described
     * in the class documentation.
     *
     * The result of this function is a map of (analyzed) query strings
     * with their respective lists of boosted query terms. These are
     * ConstantScoreQuery instances for each term with the corresponding
     * boost factor. (Invalid - i.e. non-numerical - boost factors are
     * logged as warnings).
     *
     * The SOLR core that is passed into this function is necessary for
     * determinating the FieldType of the boosted fields. Only with the
     * correct field type is it possible to boost non-string fields, as
     * these non-string values need to be ft.readableToIndexed().
     *
     * @param cfg
     *            Config object to read the XML QLTB from
     * @param core
     *            SOLR Core the query is performed on
     * @return QLTB map
     *
     * @throws IOException
     *             If the query could not be analysed
     */
    private Map<String, List<Query>> loadQLTBMap(final Config cfg, final SolrCore core) throws IOException {
        Map<String, List<Query>> map = new HashMap<String, List<Query>>();
        NodeList nodes = (NodeList) cfg.evaluate("boosts/query", XPathConstants.NODESET);
        for (int i = 0; i < nodes.getLength(); i++) {
            Node node = nodes.item(i);
            String qstr = DOMUtil.getAttr(node, "text", "missing query 'text'");
            qstr = getAnalyzedQuery(qstr);
            NodeList children = node.getChildNodes();
            List<Query> termBoosts = new ArrayList<Query>();
            for (int j = 0; j < children.getLength(); j++) {
                Node child = children.item(j);
                if (!child.getNodeName().equals("term")) {
                    continue;
                }
                String field = DOMUtil.getAttr(child, "field", "missing 'field'");
                String value = DOMUtil.getAttr(child, "value", "missing 'value'");
                String boost = DOMUtil.getAttr(child, "boost", "missing 'boost'");
                float termBoost = 1;
                try {
                    termBoost = Float.parseFloat(boost);
                } catch (NumberFormatException e) {
                    log.warn(
                        "invalid boost " + boost + " for query \"" + qstr
                        + "\", term: \"" + field + ":" + value + "\": "
                        +  e.getMessage()
                    );
                    continue;
                }
                // without readableToIndexed QLTB boosting would only work
                // for string field types
                FieldType ft = core.getLatestSchema().getField(field).getType();
                value = ft.readableToIndexed(value);
                Term t = new Term(field, value);
                TermQuery tq = new TermQuery(t);
                ConstantScoreQuery csq = new ConstantScoreQuery(tq);
                csq.setBoost(termBoost);
                termBoosts.add(csq);
            }
            map.put(qstr, termBoosts);
        }
        return map;
    }

    /**
     * Check if this component is disabled for this particular request.
     * This component can be disabled on a per-request basis by either
     * adding qltb=false (substitute "qltb" with configured component name)
     * or adding opt_out=qltb (substitute...) to the SOLR request.
     */
    private final boolean disabled(final ResponseBuilder responseBuilder) {
        SolrParams requestParams = responseBuilder.req.getParams();
        String componentName = initArgs.get(COMPONENT_NAME);
        String disable = requestParams.get(DISABLE_COMPONENT_PARAM);
        if (disable != null
            && Arrays.asList(disable.split(",")).contains(componentName)) {
            return true;
        }
        return requestParams.getBool(componentName, false);
    }

    /**
     * Add boost terms to the query if it matches a know query.
     *
     * The query string is analyzed and compared to the known query strings
     * from the XML file. If a matching (i.e. equal) query string is found,
     * the associated terms (ConstantScoreQuery) are added: the original
     * query (object, not string) is added as a MUST term in a newly created
     * BooleanQuery, whereas the new terms are added either as Occur.SHOULD
     * for positive boost, or Occur.MUST_NOT for zero or negative boost.
     *
     * prepare() might trigger a reload of the XML file if it resides in
     * the data/ directory and the reader is new.
     *
     */
    @Override
    public final void prepare(final ResponseBuilder rb) {
        if (disabled(rb)) {
            return;
        }
        Query query = rb.getQuery();
        String queryStr = rb.getQueryString();
        if (query == null || queryStr == null) {
            return;
        }
        IndexReader reader = rb.req.getSearcher().getIndexReader();
        List<Query> boostTerms = null;
        try {
            queryStr = getAnalyzedQuery(queryStr);
            boostTerms = getQLTBMap(reader, rb.req.getCore()).get(queryStr);
            if (boostTerms == null || boostTerms.isEmpty()) {
                return;
            }
            log.debug(
                "QLTBComponent.prepare() query: \"" + queryStr + "\" with "
                + boostTerms.size() + " boost terms"
            );
        } catch (Exception ex) {
            throw new SolrException(
                SolrException.ErrorCode.SERVER_ERROR,
                "error loading QLTB",
                ex
            );
        }
        BooleanQuery newq = new BooleanQuery(true);
        newq.add(query, BooleanClause.Occur.MUST);
        for (Query term : boostTerms) {
            if (term.getBoost() > 0.0) {
                newq.add(new BooleanClause(term, BooleanClause.Occur.SHOULD));
            } else {
                newq.add(new BooleanClause(term, BooleanClause.Occur.MUST_NOT));
            }
        }
        rb.setQuery(newq);
    }

    @Override
    public void process(final ResponseBuilder rb) throws IOException {
    }

    @Override
    public final String getDescription() {
        return "Query-Local Term Boost component";
    }

    @Override
    public final String getSource() {
        return "com/billiger/solr/handler/component/QLTBComponent.java";
    }

}
