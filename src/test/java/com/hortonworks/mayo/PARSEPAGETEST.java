package com.hortonworks.mayo;

import java.io.IOException;

import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

public class PARSEPAGETEST {
	public static void main(String[] args) {
		PARSEPAGE parser = new PARSEPAGE();
		String xml = "<page>\n  <title>User:AlbaniaGovernment</title>\n  <ns>0</ns>\n  <id>35</id>\n  <redirect title=\"Politics of Albania\" />\n  <revision>\n    <id>74467128</id>\n    <parentid>15898965</parentid>\n    <timestamp>2006-09-08T04:19:45Z</timestamp>\n    <contributor>\n      <username>Rory096</username>\n      <id>750223</id>\n    </contributor>\n    <comment>cat rd</comment>\n    <text xml:space=\"preserve\">#REDIRECT [[Politics of Albania]] {{R from CamelCases}}\n1\n2\n3</text>\n    <sha1>9la5osjxpu0aysereletnbv44up2k8j</sha1>\n    <model>wikitext</model>\n    <format>text/x-wiki</format>\n  </revision>\n</page>";
		TupleFactory tf = TupleFactory.getInstance();
		Tuple t = tf.newTuple();
		t.append(xml);
		try {
			Tuple output = parser.exec(t);
			System.out.println(output);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
