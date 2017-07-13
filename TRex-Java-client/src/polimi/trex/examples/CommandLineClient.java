//
// This file is part of T-Rex, a Complex Event Processing Middleware.
// See http://home.dei.polimi.it/margara
//
// Authors: Gianpaolo Cugola, Daniele Rogora
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with this program.  If not, see http://www.gnu.org/licenses/.
//

package polimi.trex.examples;

import java.util.List;
import java.util.Map;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Date;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.io.File;
import java.io.FileInputStream;

import polimi.trex.common.Attribute;
import polimi.trex.common.Consts.EngineType;
import polimi.trex.communication.PacketListener;
import polimi.trex.communication.TransportManager;
import polimi.trex.packets.PubPkt;
import polimi.trex.packets.RulePkt;
import polimi.trex.packets.SubPkt;
import polimi.trex.packets.TRexPkt;
import polimi.trex.ruleparser.TRexRuleParser;

/**
 *
 * A very basic, command line oriented, client for TRex.
 *
 * Simple setup config:
 * <ol>
 *   <li>start a TRexServer in one shell</li>
 *   <li>use the client to inject the rules as defined in your PDF, with command <code>$ java -jar TRex-client.jar
 *   localhost 50254 -rule trex.rules</code> and content
 *   <pre>
 *      Assign 2000 => Smoke, 2001 => Temp, 2100 => Fire
 *
 *      Define  Fire(area: string, measuredTemp: double)
 *      From    Smoke(area=>$a) and each Temp([string]area=$a, value>45) within 300000 from Smoke
 *      Where   area:=Smoke.area, measuredTemp:=Temp.value;
 *   </pre>
 *   </li>
 *   <li>start a listening client in a dedicated shell with command
 *    <code>$ java -jar TRex-client.jar localhost 50254 -sub 2100</code></li>
 *   <li>inject events with commands:
 *    <code>$ java -jar TRex-client.jar localhost 50254 -pub 2001 area toto value 50</code>
 *    <code>$ java -jar TRex-client.jar localhost 50254 -pub 2000 area toto</code>
 *   </li>
 *   <li>the second event generate a pub packet on the client side.</li>
 * </ol>
 *
 * Note that all subsequent calls will generate as many output events as they are temperature events above 45 for each
 * smoke event (CEP remembers the timestamp of the event, so as many temp events in the 5min frame)
 *
 * @author Gianpaolo Cugola, Daniele Rogora, Fabian Gilson
 */
public class CommandLineClient implements PacketListener {
  static String teslaRule;
  static String readFile(String path, Charset encoding) throws IOException {
    File file = new File(path);
    FileInputStream fis = new FileInputStream(file);
    byte[] encoded = new byte[(int) file.length()];
    fis.read(encoded);
    fis.close();
    return encoding.decode(ByteBuffer.wrap(encoded)).toString();
  }

  private TransportManager tManager = new TransportManager(true);


  public static void main(String[] args) throws IOException {
    String serverHost = null;
    int serverPort = -1;
    List<Integer> subscriptions = new ArrayList<>();
    Map<Integer, Map<String,String>> notifications = new HashMap<>();

    CommandLineClient client;
    int i = 0;
    Boolean sendRule = false;
    try {
      
      if(args.length<2) {
        printUsageAndExit();
      }
      
      serverHost = args[i++];
      serverPort = Integer.parseInt(args[i++]);
      while(i<args.length) {
        // System.out.println("current arg at " + i + ": " + args[i]);
        if(i<args.length && args[i].equals("-pub")) {
          i++;
          Integer current = Integer.parseInt(args[i++]);
          notifications.put(current, new HashMap<>());
          while(i<args.length && !args[i].startsWith("-")) {
            notifications.get(current).put(args[i++], args[i++]);
            // System.out.println("read key-value pair: " notifications.get(current).toString());
          }
        }
        if(i<args.length && args[i].equals("-sub")) {
          i++;
          while(i<args.length && !args[i].startsWith("-")) {
            subscriptions.add(Integer.parseInt(args[i++]));
          }
        }
        if(i<args.length && args[i].equals("-rule")) {
          i++;
          sendRule = true;
          teslaRule = readFile(args[i], Charset.defaultCharset());
          i++;
        }
      }
    } catch(NumberFormatException e) {
      System.out.println("Error at parameter "+i);
      printUsageAndExit();
    }
    try {
      client = new CommandLineClient(serverHost, serverPort);
      if(subscriptions.size()>0) {
        client.tManager.addPacketListener(client);
        client.tManager.start();
        client.subscribe(subscriptions);
      }
      if (sendRule) client.sendRule();
      for (Integer key : notifications.keySet()) {
        client.publish(key, new ArrayList<>(notifications.get(key).keySet()), new ArrayList<>(notifications.get(key).values()));
      }
    } catch(IOException e) {
      e.printStackTrace();
    }
  }

  private static void printUsageAndExit() {
    System.out.println("Usage: java -jar TRexClient-JavaEx.jar "+
        "<server_host> <server_port> "+
        "[-rule path/to/file]"+
        "[-sub <evt_type_1> ... <evt_type_n>]"+
        "[-pub <evt_type> [<key_1> <val_1> ... <key_n> <val_n>]]");
    System.exit(-1);
  }

  public CommandLineClient(String serverHost, int serverPort) throws IOException {
    tManager.connect(serverHost, serverPort);
  }

  public void sendRule() {
    RulePkt rule = TRexRuleParser.parse(teslaRule, 2000);
    try {
      tManager.sendRule(rule, EngineType.CPU);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  public void subscribe(List<Integer> subTypes) {
    for(int subType : subTypes) {
      SubPkt sub = new SubPkt(subType);
      try {
        tManager.send(sub);
      } catch (IOException e) { 
        e.printStackTrace(); 
      }
    }
  }

  public void publish(int pubType, List<String> keys, List<String> values) {
    PubPkt pub;
    boolean boolVal;
    int intVal;
    float floatVal;

    pub = new PubPkt(pubType);
    for(int i=0; i<keys.size(); i++) {
      if(values.get(i).equals("true")) {
        boolVal = true;
        pub.addAttribute(new Attribute(keys.get(i), boolVal)); // add a bool attr
      } else if(values.get(i).equals("false")) {
        boolVal = false;
        pub.addAttribute(new Attribute(keys.get(i), boolVal)); // add a bool attr
      } else {
        try {
          intVal = Integer.parseInt(values.get(i));
          pub.addAttribute(new Attribute(keys.get(i), intVal)); // add an int attr
        } catch(NumberFormatException e1) {
          try {
            floatVal = Float.parseFloat(values.get(i));
            pub.addAttribute(new Attribute(keys.get(i), floatVal)); // add a float attr
          } catch(NumberFormatException e2) {
            pub.addAttribute(new Attribute(keys.get(i), values.get(i))); // add a String attr
          }
        }
      }
    }
    try {
      tManager.send(pub);
    } catch (IOException e) { 
      e.printStackTrace(); 
    }
  }

  @Override
  public void notifyPktReceived(TRexPkt pkt) {
    if(! (pkt instanceof PubPkt)) {
      System.out.println("Ingnoring wrong packet: "+pkt);
      return;
    }
    PubPkt pub = (PubPkt) pkt;
    System.out.print("PubPacket received: {");
    System.out.print(pub.getEventType());
    for(Attribute att : pub.getAttributes()) {
      System.out.print(" <"+att.getName());
      switch(att.getValType()) {
        case BOOL: System.out.print(" : bool = "+att.getBoolVal()+">"); break;
        case INT: System.out.print(" : int = "+att.getIntVal()+">"); break;
        case FLOAT: System.out.print(" : float = "+att.getFloatVal()+">"); break;
        case STRING: System.out.print(" : string = "+att.getStringVal()+">"); break;
      }
    }
    System.out.print("}@");
    System.out.println(new Date(pub.getTimeStamp()).toLocaleString());
  }
  @Override
  public void notifyConnectionError() {
    System.out.println("Connection error. Exiting.");
    System.exit(-1);
  }
}
    
