package com.hortonworks.mayo.ctakes.cr;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

import org.apache.ctakes.typesystem.type.structured.DocumentID;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.uima.cas.CAS;
import org.apache.uima.cas.CASException;
import org.apache.uima.collection.CollectionException;
import org.apache.uima.collection.CollectionReader_ImplBase;
import org.apache.uima.jcas.JCas;
import org.apache.uima.resource.ResourceInitializationException;
import org.apache.uima.resource.ResourceSpecifier;
import org.apache.uima.util.Progress;
import org.apache.uima.util.ProgressImpl;

/**
 * Component to plug into cTAKES for processing Tuples from Pig.
 * 
 * @author Paul Codding - paul@hortonworks.com
 * 
 */
public class TuplesCollectionReader extends CollectionReader_ImplBase {
	DataBag bagToProcess = null;
	long numTuplesProcessed = 0;
	Iterator<Tuple> bagIterator = null;

	public void getNext(CAS aCas) throws IOException, CollectionException {
		JCas jcas;
		try {
			Tuple nextTuple = bagIterator.next();
			jcas = aCas.getJCas();
			DocumentID documentIDAnnotation = new DocumentID(jcas);
			String docID = (String) nextTuple.get(0);
			documentIDAnnotation.setDocumentID(docID);
			documentIDAnnotation.addToIndexes();

			String text = (String) nextTuple.get(1);
			jcas.setDocumentText(text);
		} catch (CASException e) {
			e.printStackTrace();
		} finally {
			numTuplesProcessed++;
		}
	}

	public void close() throws IOException {
	}

	public Progress[] getProgress() {
		return new Progress[] { new ProgressImpl((int) numTuplesProcessed,
				(int) bagToProcess.size(), Progress.ENTITIES) };
	}

	public boolean hasNext() throws IOException, CollectionException {
		if (bagToProcess == null)
			return true;
		return numTuplesProcessed < bagToProcess.size();
	}

	public DataBag getBagToProcess() {
		return bagToProcess;
	}

	public void setBagToProcess(DataBag bagToProcess) {
		this.bagToProcess = bagToProcess;
		bagIterator = bagToProcess.iterator();
	}
}
