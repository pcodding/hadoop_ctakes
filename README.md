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

# Building
This is a maven project, and can be built using `mvn clean package`.  The jar will be registered within the Pig script.

# Deploying to the Hortonworks Sandbox 1.3

## Sandbox Preparation

Because this code should be run as a non-privileged user, one must first be created on the sandbox, and in HDFS.

	# adduser –g 100 mayoclinic
	# passwd mayoclinic
	# /usr/lib/hadoop/sbin/hadoop-create-user.sh mayoclinic
	
We'll also need to add SVN to checkout the Apache cTAKES code.
	# yum -y install svn
	
## Code Dependencies

The Hadoop code depends on two utilities: Git, and Maven.  Git is already installed on the Sandbox, but Maven is not.  Follow the steps below to install Maven.

	# su - mayoclinic 
	$ wget http://www.eng.lsu.edu/mirrors/apache/maven/maven-3/3.0.5/binaries/apache-maven-3.0.5-bin.tar.gz
	$ tar –zxvf apache-maven-3.0.5-bin.tar.gz
	$ ln -s apache-maven-3.0.5 maven
	$ echo export 'PATH=${PATH}:/home/mayoclinic/maven/bin' >> ~/.bash_profile
	$ . ~/.bash_profile
	
## Testing Maven
Once we've sourced our .bash_profile file in the last step above, we can make sure maven is working by issuing the following command and verifiying the output.

	$ mvn -version
	Apache Maven 3.0.5 (r01de14724cdef164cd33c7c8c2fe155faf9602da; 2013-02-19 05:51:28-0800)
	Maven home: /home/mayoclinic/maven
	Java version: 1.6.0_31, vendor: Sun Microsystems Inc.
	Java home: /usr/jdk/jdk1.6.0_31/jre
	Default locale: en_US, platform encoding: UTF-8
	OS name: "linux", version: "2.6.32-358.6.2.el6.x86_64", arch: "amd64", family: "unix"

## Getting the Code
The code for the cTAKES Pig UDF's is in Github at the following location:
https://github.com/pcodding/hadoop_ctakes.  We'll be pulling the code down using git and building it by following the steps below.

	$ mkdir ~/src
	$ cd ~/src
	$ git clone https://github.com/pcodding/hadoop_ctakes.git
	$ cd hadoop_ctakes

## Adding Maven Dependencies

### cTAKES Dependencies

We'll need to checkout and install the cTAKES 3.1.0-SNAPSHOT code in order to build the PIG UDFs. It's important to note that we need a specific revision of trunk  noted below:

	$ cd ~/src
	$ svn co https://svn.apache.org/repos/asf/ctakes/trunk/@1499008

In order to conserve space, we'll be building/installing select cTAKES packages, then removing the source code.

	$ cd trunk
	$ mvn -Dmaven.test.skip=true install
	$ mvn clean
	$ cd ~/src
	$ rm –rf trunk
	
## Updating the Configuration

The Pig UDF's need to know where the cTAKES config and resources are located.  We'll make a local copy of these resources, then update the configuration to point to that local copy before building the project.

	$ cd ~/src/hadoop_ctakes
	$ mkdir /tmp/ctakes_config
	$ cp -r ctakes_config/* /tmp/ctakes_config
	$ chmod -R 777 /tmp/ctakes_config
	$ echo 'configBasePath=/tmp/ctakes_config' > src/main/resources/config.properties

## Building the Code

Now that all of our dependencies have been installed, we can build the Pig UDF's using Maven.

	$ cd ~/src/hadoop_ctakes
	$ mvn clean install

## Creating the HCatalog tables

	$ hive
	hive> CREATE TABLE wikipedia_pages(title STRING, text STRING)
	STORED AS SEQUENCEFILE;
	hive> CREATE TABLE wikipedia_pages_annotated(title STRING, text STRING, annotations STRING)
	PARTITIONED BY (loaded STRING)
	STORED AS SEQUENCEFILE;
	hive> quit;
	
## Copy Sample Data Into Cluster

A few sample articles are included in the project under ./sample_data.  We'll add this data to the cluster using the following commands.

	$ hadoop fs -mkdir ./wikipedia
	$ hadoop fs -put ~/src/hadoop_ctakes/sample_data/* ./wikipedia
	$ hadoop fs -ls ./wikipedia
	Found 1 items
	-rwx------   1 mayoclinic mayoclinic      87958 2013-06-25 23:48 /user/mayoclinic/wikipedia/enwiki-latest-pages-meta-current.xml.bz2

## Running the Pig Scripts on Sample Data

To create an area in which we can stage our Pig scripts and dependent Jars, we are going to create a pig directory and copy in our scripts and jars to it.

	$ mkdir ~/pig
	$ cd ~/pig
	$ cp ~/src/hadoop_ctakes/pig/p* .
	$ cp ~/src/hadoop_ctakes/target/*.jar .
The first pig script parses the bzip2 compressed XML and stores it in the wikipedia_pages HCatalog table.  This script can be simply run using the following command.

	$ cd ~/pig
	$ pig –useHCatalog parse_pages.pig

The pig job will run to completion and let you know that 12 records were written.  Now we'll make sure everything looks as it should, and confirm that the pages were parsed and placed in our wikipedia_pages table.

	$ hive -e 'select title from wikipedia_pages'
	…
	AccessibleComputing
	Anarchism
	AfghanistanHistory
	AfghanistanGeography
	AfghanistanPeople
	AfghanistanCommunications
	AfghanistanTransportations
	AfghanistanMilitary
	AfghanistanTransnationalIssues
	AssistiveTechnology
	AmoeboidTaxa
	Autism
	Time taken: 11.106 seconds, Fetched: 12 row(s)

To confirm we have the text parsed as well, we can run the following query:

	$ hive -e "select title,text from wikipedia_pages where title ='Anarchism'" 

Because there are so many jars that are required to run cTAKES, a shell script is required to populate all of the dependencies to be passed to pig.  We'll use a ruby one liner to build all of the arguments that need to be passed to ensure the jars are visible to pig.  This one liner will build a shell script that we can use to invoke pig with all of the necessary jars.

	$ ruby -e 'jars="";Dir.glob("/home/mayoclinic/src/hadoop_ctakes/target/dependencies/*.jar") { |jar| jars += ":#{jar}"};Dir.glob("/home/mayoclinic/src/hadoop_ctakes/lib/*.jar") { |jar| jars += ":#{jar}"};puts "#{jars} $1"' | sed 's/^:/pig -useHCatalog -Dpig.additional.jars=/' > ~/pig/pig_ctakes.sh && chmod u+x ~/pig/pig_ctakes.sh

At this point you will now have an executable shell script called pig_ctakes.sh in the ~/pig directory that we'll use to execute the script that uses the PROCESSPAGE UDF.  We'll invoke this script that will read all of the records from the wikipedia_pages table, send the title and text through cTAKES and store the title, text, and CAS output to the wikipedia_pages_annotated table in HCatalog.

	$ cd ~/pig
	$ ./pig_ctakes.sh process_pages.pig
	
Once this has been run, you can use the following hive statement to look at the annotations produced by the pig script:

	hive -e "select title,text,annotations from wikipedia_pages_annotated where title ='Anarchism'" 