-- Process only Hortonworks Wikipedia page
set mapred.job.map.memory.mb 8096;
set mapred.cluster.map.memory.mb 8096;
set mapred.child.java.opts -Xmx6144m;
set mapred.output.compress true;
set hive.exec.compress.output true;

register /usr/lib/pig/piggybank.jar;
register hadoop_ctakes-0.0.1-SNAPSHOT.jar

DEFINE PROCESSPAGE com.hortonworks.mayo.ctakes.PROCESSPAGE();

ARTICLES = load 'wikipedia_pages' using org.apache.hcatalog.pig.HCatLoader();
FILTERED_ARTICLES = FILTER ARTICLES BY (title matches 'Hortonworks');
ANNOTATED_ARTICLES = FOREACH FILTERED_ARTICLES GENERATE FLATTEN(PROCESSPAGE($0, $1));
store ANNOTATED_ARTICLES into 'wikipedia_pages_annotated' using org.apache.hcatalog.pig.HCatStorer('loaded=2013070101', 'title: chararray,text: chararray,annotations: chararray');

-- Process ALL Wikipedia pages
set mapred.job.map.memory.mb 8096;
set mapred.cluster.map.memory.mb 8096;
set mapred.child.java.opts -Xmx7256m;
set mapred.output.compress true;
set hive.exec.compress.output true;

register /usr/lib/pig/piggybank.jar;
register hadoop_ctakes-0.0.1-SNAPSHOT.jar

DEFINE PROCESSPAGE com.hortonworks.mayo.ctakes.PROCESSPAGE();

ARTICLES = load 'wikipedia_pages' using org.apache.hcatalog.pig.HCatLoader();
ANNOTATED_ARTICLES = FOREACH ARTICLES GENERATE FLATTEN(PROCESSPAGE($0, $1));
store ANNOTATED_ARTICLES into 'wikipedia_pages_annotated' using org.apache.hcatalog.pig.HCatStorer('loaded=2013070101', 'title: chararray,text: chararray,annotations: chararray');
