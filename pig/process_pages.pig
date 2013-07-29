register /usr/lib/pig/piggybank.jar;
register hadoop_ctakes-0.0.1-SNAPSHOT.jar;

DEFINE PROCESSPAGE com.hortonworks.mayo.ctakes.PROCESSPAGE();

ARTICLES = load 'wikipedia_pages' using org.apache.hcatalog.pig.HCatLoader();
F_ARTICLES = FILTER ARTICLES BY (title matches 'Anarchism');
ANNOTATED_ARTICLES = FOREACH F_ARTICLES GENERATE FLATTEN(PROCESSPAGE($0, $1));
store ANNOTATED_ARTICLES into 'wikipedia_pages_annotated' using org.apache.hcatalog.pig.HCatStorer('loaded=2013070100', 'title: chararray,text: chararray,annotations: chararray');