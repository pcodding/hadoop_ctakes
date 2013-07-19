package com.hortonworks.mayo.ctakes;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.util.List;

import org.apache.ctakes.core.util.DocumentIDAnnotationUtil;
import org.apache.pig.EvalFunc;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.PigLogger;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;
import org.apache.uima.UIMAFramework;
import org.apache.uima.cas.CAS;
import org.apache.uima.cas.CASException;
import org.apache.uima.cas.impl.XCASSerializer;
import org.apache.uima.collection.CollectionProcessingEngine;
import org.apache.uima.collection.EntityProcessStatus;
import org.apache.uima.collection.StatusCallbackListener;
import org.apache.uima.collection.metadata.CpeDescription;
import org.apache.uima.jcas.JCas;
import org.apache.uima.resource.ResourceInitializationException;
import org.apache.uima.util.InvalidXMLException;
import org.apache.uima.util.XMLInputSource;
import org.xml.sax.SAXException;

import com.hortonworks.mayo.ConfigUtil;
import com.hortonworks.mayo.ctakes.cr.TuplesCollectionReader;

/**
 * Pig UDF to process Wikipedia pages through cTAKES.
 * 
 * @author Paul Codding - paul@hortonworks.com
 * 
 */
public class PROCESSPAGE extends EvalFunc<Tuple> {
	private static final int MAX_TIMEOUT_MS = 10 * 60 * 1000; // 10 mins
	TupleFactory tf = TupleFactory.getInstance();
	BagFactory bf = BagFactory.getInstance();
	long numTuplesProcessed = 0;
	CpeDescription cpeDesc = null;

	/**
	 * Initialize the CpeDescription class.
	 */
	private void initializeFramework() {
		try {
			if (cpeDesc == null)
				cpeDesc = UIMAFramework.getXMLParser().parseCpeDescription(
						new XMLInputSource(this.getClass().getClassLoader()
								.getResourceAsStream("PipelineCPE.xml"),
								new File(ConfigUtil.getConfigBasePath())));
		} catch (InvalidXMLException e) {
			log.warn(e.getMessage());
			e.printStackTrace();
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.apache.pig.EvalFunc#exec(org.apache.pig.data.Tuple)
	 */
	@Override
	public Tuple exec(Tuple input) throws IOException {
		long started = System.currentTimeMillis();
		Tuple result = null;
		try {
			System.out.println(input.get(0) + ": Reported progress");
			progress();
			initializeFramework();
			CollectionProcessingEngine mCPE = UIMAFramework
					.produceCollectionProcessingEngine(cpeDesc);
			// Process the tuples
			System.out.println(input.get(0)
					+ ": Invoked CollectionProcessingEngine");
			DataBag bagToProcess = bf.newDefaultBag();
			bagToProcess.add(input);
			((TuplesCollectionReader) mCPE.getCollectionReader())
					.setBagToProcess(bagToProcess);
			System.out.println(input.get(0)
					+ ": Added Tuples to TuplesCollectionReader");
			mCPE.process();
			StatusCallbackListenerImpl t = new StatusCallbackListenerImpl(
					this.pigLogger);
			mCPE.addStatusCallbackListener(t);
			System.out.println(input.get(0) + ": Registered callback handler");
			while (!t.isComplete()) {
				if (System.currentTimeMillis() - started >= MAX_TIMEOUT_MS) {
					System.err.append(input.get(0) + ": Task timed out");
					t.setComplete(true);
					result = input;
					result.append("");
					t.setResult(result);
				}
				Thread.sleep(500);
			}
			result = t.getResult();
			System.out.println(input.get(0) + ": Completed processing in "
					+ Long.toString(System.currentTimeMillis() - started));
		} catch (ResourceInitializationException e) {
			log.error(e.getMessage());
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		return result;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * org.apache.pig.EvalFunc#outputSchema(org.apache.pig.impl.logicalLayer
	 * .schema.Schema)
	 */
	@Override
	public Schema outputSchema(Schema input) {
		try {
			Schema tupleSchema = new Schema();
			tupleSchema.add(new FieldSchema("title", DataType.CHARARRAY));
			tupleSchema.add(new FieldSchema("text", DataType.CHARARRAY));
			tupleSchema.add(new FieldSchema("annotations", DataType.CHARARRAY));
			return tupleSchema;
		} catch (Exception e) {
			return null;
		}
	}

	/**
	 * Callback listener to handle document lifecycle processing events.
	 * 
	 * @author Paul Codding - paul@hortonworks.com
	 * 
	 */
	class StatusCallbackListenerImpl implements StatusCallbackListener {
		long size = 0;
		PigLogger logger;
		boolean complete = false;
		Tuple result = null;

		public StatusCallbackListenerImpl(PigLogger logger) {
			this.logger = logger;
		}

		/**
		 * Called when the initialization is completed.
		 * 
		 * @see org.apache.uima.collection.processing.StatusCallbackListener#initializationComplete()
		 */
		public void initializationComplete() {
		}

		/**
		 * Called when the batchProcessing is completed.
		 * 
		 * @see org.apache.uima.collection.processing.StatusCallbackListener#batchProcessComplete()
		 * 
		 */
		public void batchProcessComplete() {
			complete = true;
			System.out
					.println("Completed " + numTuplesProcessed + " documents");
		}

		/**
		 * Called when the collection processing is completed.
		 * 
		 * @see org.apache.uima.collection.processing.StatusCallbackListener#collectionProcessComplete()
		 */
		public void collectionProcessComplete() {
			complete = true;
			System.out
					.println("Completed " + numTuplesProcessed + " documents");
		}

		/**
		 * Called when the CPM is paused.
		 * 
		 * @see org.apache.uima.collection.processing.StatusCallbackListener#paused()
		 */
		public void paused() {
			System.out.println("Paused");
		}

		/**
		 * Called when the CPM is resumed after a pause.
		 * 
		 * @see org.apache.uima.collection.processing.StatusCallbackListener#resumed()
		 */
		public void resumed() {
			System.out.println("Resumed");
		}

		/**
		 * Called when the CPM is stopped abruptly due to errors.
		 * 
		 * @see org.apache.uima.collection.processing.StatusCallbackListener#aborted()
		 */
		public void aborted() {
			complete = true;
			System.err.println("Aborted");
		}

		/**
		 * Called when the processing of a Document is completed. <br>
		 * The process status can be looked at and corresponding actions taken.
		 * 
		 * @param aCas
		 *            CAS corresponding to the completed processing
		 * @param aStatus
		 *            EntityProcessStatus that holds the status of all the
		 *            events for aEntity
		 */
		public void entityProcessComplete(CAS aCas, EntityProcessStatus aStatus) {
			if (aStatus.isException()) {
				List<?> exceptions = aStatus.getExceptions();
				for (int i = 0; i < exceptions.size(); i++) {
					((Throwable) exceptions.get(i)).printStackTrace();
					Throwable ex = ((Throwable) exceptions.get(i));
				}
				return;
			} else {
				Tuple t = tf.newTuple();
				String documentId;
				try {
					JCas jCas = aCas.getJCas();
					documentId = DocumentIDAnnotationUtil.getDocumentID(jCas);
					ByteArrayOutputStream out = new ByteArrayOutputStream();
					XCASSerializer.serialize(aCas, out, true);
					String annotations = out.toString();
					String documentText = aCas.getDocumentText();
					// Strip newlines
					documentText = documentText.replace("\n", " ");
					annotations = annotations.replace("\n", "");
					t.append(documentId);
					t.append(documentText);
					t.append(annotations);
					result = t;
				} catch (CASException e) {
					e.printStackTrace();
				} catch (SAXException e) {
					e.printStackTrace();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}

			numTuplesProcessed++;
		}

		/**
		 * Is the document processing complete?
		 * 
		 * @return
		 */
		public boolean isComplete() {
			return complete;
		}

		/**
		 * Set this handler to be complete.
		 * 
		 * @param complete
		 */
		public void setComplete(boolean complete) {
			this.complete = complete;
		}

		/**
		 * Get the tuple containing the annotations.
		 * 
		 * @return
		 */
		public Tuple getResult() {
			return result;
		}

		public void setResult(Tuple result) {
			this.result = result;
		}
	}
}