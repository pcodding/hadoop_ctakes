cTAKES integration code for Hadoop
=============

This project contains code that will allow cTAKES to be invoked on clinical document data presented as Tuples to [cTAKES](http://ctakes.apache.org).  There are two UDF's:

# Pig UDF's

* PARSEPAGE - An example XML parsing UDF focusing on parsing Wikipedia pages for storage in HCatalog
* PROCESSPAGE - An UDF focussed on processing clinical documents through cTAKES.  The UDF uses a custom implementation of the `CollectionReader_ImplBase` class to process Tuples through cTAKES.

# Pig Scripts
The `./pig` folder contains two pig scripts used to invoke the two UDFs:

* parse_page.pig - Invokes the PARSEPAGE UDF for parsing Wikipedia articles and stores in HCatalog
* process_pages.pig - Process pages from HCatalog through the PROCESSPAGE UDF, storing them back to HCatalog for upstream analytics.