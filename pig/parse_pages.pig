register /usr/lib/pig/piggybank.jar;
register hadoop_ctakes-0.0.1-SNAPSHOT.jar;

DEFINE PARSEPAGE com.hortonworks.mayo.PARSEPAGE();

PAGE = load '/user/mayoclinic/wikipedia/enwiki-latest-pages-meta-current.xml.bz2' using org.apache.pig.piggybank.storage.XMLLoader('page') as (pageContent: chararray);
TITLE_TEXT = FOREACH PAGE GENERATE FLATTEN(PARSEPAGE(pageContent)) as (title: chararray, text: chararray);
ARTICLES = FILTER TITLE_TEXT BY (title matches '^(?!(Media|Special|Talk|User|User talk|Wikipedia|Wikipedia talk|File|File talk|MediaWiki|MediaWiki talk|Template|Template talk|Help|Help talk|Category|Category talk|Portal|Portal talk|Book|Book talk):).*');
store ARTICLES into 'wikipedia_pages' using org.apache.hcatalog.pig.HCatStorer();