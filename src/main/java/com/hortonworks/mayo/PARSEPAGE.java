package com.hortonworks.mayo;

import java.io.IOException;
import java.io.StringReader;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.xml.sax.Attributes;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

/**
 * Pig UDF to parse Wikipedia enwiki-latest-pages-meta-current.xml.bz2 data
 * 
 * @author Paul Codding - paul@hortonworks.com
 */
public class PARSEPAGE extends EvalFunc<Tuple> {
	SAXParserFactory factory = SAXParserFactory.newInstance();
	SAXParser saxParser = null;
	WikipediaXmlHandler handler = null;

	public PARSEPAGE() {
		try {
			factory.newSAXParser();
		} catch (ParserConfigurationException e) {
			e.printStackTrace();
		} catch (SAXException e) {
			e.printStackTrace();
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.apache.pig.EvalFunc#exec(org.apache.pig.data.Tuple)
	 */
	public Tuple exec(Tuple input) throws IOException {
		String textValue = null;
		String titleValue = null;

		handler = new WikipediaXmlHandler();
		TupleFactory tf = TupleFactory.getInstance();
		if (input == null || input.size() == 0)
			return null;
		try {
			// Pig will pass in a tuple with a single string value of the <page>...</page> content.
			String xml = (String) input.get(0);
			
			// Parse the pages data with our handler
			saxParser = factory.newSAXParser();
			saxParser.parse(new InputSource(new StringReader(xml)), handler);
			
			titleValue = handler.getTitleValue();
			// Strip newlines from text body
			textValue = handler.getTextValue().replaceAll("\\n", " ");
			
			// Build the tuple to be returned
			Tuple t = tf.newTuple();
			t.append(titleValue);
			t.append(textValue);
			return t;
		} catch (Exception e) {
			return tf.newTuple();
		}
	}
}

/**
 * Parse the <page/> content passed in by Pig, and parse out the title and the
 * text.
 * 
 * @author paul
 * 
 */
class WikipediaXmlHandler extends DefaultHandler {
	boolean title = false;
	boolean text = false;
	String titleValue = null;
	String textValue = new String();

	public void startElement(String uri, String localName, String qName,
			Attributes attributes) throws SAXException {
		if (qName.equalsIgnoreCase("title")) {
			title = true;
		}
		if (qName.equalsIgnoreCase("text")) {
			text = true;
		} else
			text = false;
	}

	public void endElement(String uri, String localName, String qName)
			throws SAXException {
		text = false;
	}

	public void characters(char ch[], int start, int length)
			throws SAXException {

		if (title) {
			titleValue = new String(ch, start, length);
			title = false;
		}

		if (text) {
			textValue += new String(ch, start, length);
		}
	}

	public String getTitleValue() {
		return titleValue;
	}

	public String getTextValue() {
		return textValue;
	}
}