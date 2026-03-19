package SiminStormScheduler.Calculations;

import static SiminStormScheduler.Calculations.Utility.JainIndex;
import static SiminStormScheduler.Calculations.Utility.NetworkUsageNode;

public class Main {

    public static void main(String[] args) {
        System.out.println("Hello world!");

        //Jain network
//        int m = 3;// number of nodes
//        double x[] = {829.39,388.77,351.87};//network per node
//        double JainIndex = Utility.JainIndex(m, x);
//        System.out.println("JainIndex = " + JainIndex);

        //String filename = "C:\\Users\\partiran\\Desktop\\EX1. multi app\\Default\\5app\\net master.txt";
//        String filename = "C:\\Users\\partiran\\Desktop\\EX. multi nodes\\custom 116\\5 nodes\\net slave 4.txt";
        String filename = "F:\\PhD Main Project\\Working Space\\EX. multi nodes\\SiminRL\\5 nodes\\net slave 4.txt";
        double netUsage = NetworkUsageNode(2730, filename, 100);//565
        System.out.println("NetworkUsage = " + netUsage + " for " + filename );


    }
}
