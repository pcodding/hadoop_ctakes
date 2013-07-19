package com.hortonworks.mayo.ctakes;

import java.io.IOException;

import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

public class PROCESSPAGETest {
	public static void main(String[] args) {
		PROCESSPAGE pp = new PROCESSPAGE();
		try {
			TupleFactory tf = TupleFactory.getInstance();

			Tuple pageTuple = tf.newTuple();
			pageTuple.append("sample_tuple");
			pageTuple
					.append("IMPRESSION/REPORT/PLAN\nThe patient, who is very irritable, has very typical history for claudication. Yet the patient does not have any significant bruit at the pelvic area, abdominal area.\n\nAspirin 200-mg once-a-day.\nIbuprofen increased from 200 mg to 300 mg p.o. twice a day.\nAspirin 80 mg.\nacetaminophen discontinued.");
			Tuple output = pp.exec(pageTuple);
			System.out.println(output);
			
			pageTuple = tf.newTuple();
			pageTuple.append("anotherSampleTuple");
			pageTuple
					.append("\nDr. Nutritious\n\nMedical Nutrition Therapy for Hyperlipidemia\n\nReferral from: Julie Tester, RD, LD, CNSD\nPhone contact: (555) 555-1212\nHeight: 144 cm   Current Weight: 45 kg   Date of current weight: 02-29-2001   Admit Weight:  53 kg   BMI: 18 kg/m2\nDiet: General\nDaily Calorie needs (kcals): 1500 calories, assessed as HB + 20% for activity.\nDaily Protein needs: 40 grams,  assessed as 1.0 g/kg.\nPt has been on a 3-day calorie count and has had an average intake of 1100 calories.  She was instructed to drink 2-3 cans of liquid supplement to help promote weight gain.  She agrees with the plan and has my number for further assessment. May want a Resting Metabolic Rate as well. She takes an aspirin a day for knee pain.");
			output = pp.exec(pageTuple);
			System.out.println(output);
			System.out.println(output.getClass());
		} catch (IOException e) {
			e.printStackTrace();
		};
	}
}
