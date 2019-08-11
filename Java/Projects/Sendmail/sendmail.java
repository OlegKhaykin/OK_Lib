/*
 * @(#)sendmail.java	1.2 
 *
 * 
 */

import java.util.*;
import java.io.*;
import java.lang.reflect.*;
import javax.mail.*;
import javax.mail.internet.*;
import javax.activation.*;

/**
 * sendmail creates a text/plain message and sends it to an abitrary number of recipients.
 * <p>
 *
 * @author Rob Helgeson
 */

public class sendmail {

//    public static void main(String toIn, String ccIn, String bccIn, String hostIn, String subjIn, String msgTextIn, String debugIn) {
    public static void mailme(String toIn, String ccIn, String bccIn, String hostIn, String subjIn, String msgTextIn, String debugIn) {

	if ((debugIn == "") || (msgTextIn == "") || (subjIn == "") || (hostIn == "") || ((toIn == "") && (ccIn == "") && (bccIn == ""))) {
	    usage();
	    System.exit(1);
	}

	String from = "OracleDatabase@datatec.com";
	String host = hostIn;
	String subj = subjIn;
	String msgText = msgTextIn;
	boolean debug = Boolean.valueOf(debugIn).booleanValue();

	// create some properties and get the default Session
	Properties props = new Properties();
	props.put("mail.smtp.host", host);
	if (debug) props.put("mail.debug", debugIn);

	Session session = Session.getDefaultInstance(props, null);
	session.setDebug(debug);

	int i=0;

	StringTokenizer toInToken;
	StringTokenizer ccInToken;
	StringTokenizer bccInToken;
	InternetAddress[] toAddress;
	InternetAddress[] ccAddress;
	InternetAddress[] bccAddress;
	Vector tmpToAddress;
	Vector tmpCcAddress;
	Vector tmpBccAddress;

	try {
	    // create a message
	    Message msg = new MimeMessage(session);
	    msg.setFrom(new InternetAddress(from));

	    if(debug) System.out.println("toIn = " + toIn);
	    if(toIn != null){
	      if(debug) System.out.println("Entering to proccessing.");

	      toInToken = new StringTokenizer(toIn, ",");
	      toAddress = new InternetAddress[1];
	      tmpToAddress = new Vector();

	      for(i=0;toInToken.hasMoreTokens();i++){
	        tmpToAddress.addElement(new InternetAddress(toInToken.nextToken()));
	      }

	      if(debug) System.out.println("toTokenVector: " + i);
	      if(debug) System.out.println("tmpToAddress.size() = " + tmpToAddress.size());

	      toAddress = (InternetAddress[])sizeArray(toAddress, (tmpToAddress.size()));
	      tmpToAddress.copyInto(toAddress);

	      if(debug) System.out.println("toAddress[0] = " + toAddress[0]);
	      msg.setRecipients(Message.RecipientType.TO, toAddress);
	      if(debug) System.out.println("Added to's to address list");
	      if(debug) System.out.println("Exiting to proccessing.");
	    }

	    if(debug) System.out.println("ccIn = " + ccIn);
	    if(ccIn != null){
	      if(debug) System.out.println("Entering cc proccessing.");

	      ccInToken = new StringTokenizer(ccIn, ",");
	      ccAddress = new InternetAddress[1];
	      tmpCcAddress = new Vector();

	      for(i=0;ccInToken.hasMoreTokens();i++){
	        tmpCcAddress.addElement(new InternetAddress(ccInToken.nextToken()));
	      }

	      if(debug) System.out.println("ccTokenVector: " + i);
	      if(debug) System.out.println("tmpCcAddress.size() = " + tmpCcAddress.size());

	      ccAddress = (InternetAddress[])sizeArray(ccAddress, (tmpCcAddress.size()));
	      tmpCcAddress.copyInto(ccAddress);

	      if(debug) System.out.println("toAddress[0] = " + ccAddress[0]);
	      msg.setRecipients(Message.RecipientType.CC, ccAddress);
	      if(debug) System.out.println("Added cc's to address list");
	      if(debug) System.out.println("Exiting cc proccessing.");
	    }

	    if(debug) System.out.println("bccIn = " + bccIn);
	    if(bccIn != null){
	      if(debug) System.out.println("Entering bcc proccessing.");

	      bccInToken = new StringTokenizer(bccIn, ",");
	      bccAddress = new InternetAddress[1];
	      tmpBccAddress = new Vector();

	      for(i=0;bccInToken.hasMoreTokens();i++){
	        tmpBccAddress.addElement(new InternetAddress(bccInToken.nextToken()));
	      }

	      if(debug) System.out.println("bccTokenVector: " + i);
	      if(debug) System.out.println("tmpBccAddress.size() = " + tmpBccAddress.size());

	      bccAddress = (InternetAddress[])sizeArray(bccAddress, (tmpBccAddress.size()));
	      tmpBccAddress.copyInto(bccAddress);

	      if(debug) System.out.println("toAddress[0] = " + bccAddress[0]);
	      msg.setRecipients(Message.RecipientType.BCC, bccAddress);
	      if(debug) System.out.println("Added bcc's to address list");
	      if(debug) System.out.println("Exiting bcc proccessing.");
	    } 

	    if(subjIn != "") {msg.setSubject(subj);}
	    msg.setSentDate(new Date());

//	    msg.setText(msgText);
	    msg.setContent(msgText, "text/html");

	    Transport t = session.getTransport("smtp");
	    t.connect();
	    t.send(msg);

	} catch (MessagingException mex) {

	    System.out.println("\n--Exception handling in msgsendsample.java");

	    mex.printStackTrace();
	    System.out.println();
	    Exception ex = mex;
	    do {
		if (ex instanceof SendFailedException) {
		    SendFailedException sfex = (SendFailedException)ex;
		    Address[] invalid = sfex.getInvalidAddresses();
		    if (invalid != null) {
			System.out.println("    ** Invalid Addresses");
			if (invalid != null) {
			    for (i = 0; i < invalid.length; i++) 
				System.out.println("         " + invalid[i]);
			}
		    }
		    Address[] validUnsent = sfex.getValidUnsentAddresses();
		    if (validUnsent != null) {
			System.out.println("    ** ValidUnsent Addresses");
			if (validUnsent != null) {
			    for (i = 0; i < validUnsent.length; i++) 
				System.out.println("         "+validUnsent[i]);
			}
		    }
		    Address[] validSent = sfex.getValidSentAddresses();
		    if (validSent != null) {
			System.out.println("    ** ValidSent Addresses");
			if (validSent != null) {
			    for (i = 0; i < validSent.length; i++) 
				System.out.println("         "+validSent[i]);
			}
		    }
		}
		System.out.println();
	    } while ((ex = ((MessagingException)ex).getNextException()) 
		     != null);
	}
    }

    private static void usage() {
	System.out.println("usage: sendmail <comma separated list TO> <comma separated list CC> <comma separated list BCC> <smtp.host.name> <subject> <message> true|false(for debuging)");
    }

    private static Object expandArray(Object o){
      Class cl = o.getClass();
      if(!cl.isArray()) return null;
      cl = o.getClass().getComponentType();
      int arrLength = (Array.getLength(o) + 1);
      Object newArray = Array.newInstance(cl, arrLength);
      System.arraycopy(o,0,newArray,0,arrLength);
      return newArray;
    }

    private static Object sizeArray(Object o, int s){
      Class cl = o.getClass();
      if(!cl.isArray()) return null;
      cl = o.getClass().getComponentType();
      int arrLength = s;
      Object newArray = Array.newInstance(cl, arrLength);
//      System.arraycopy(o,0,newArray,0,arrLength);
      return newArray;
    }
}
