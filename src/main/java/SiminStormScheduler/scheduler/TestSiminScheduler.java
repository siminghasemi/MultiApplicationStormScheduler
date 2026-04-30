package SiminStormScheduler.scheduler;

import SiminStormScheduler.DSPModels.DSPRequirementModel;
import SiminStormScheduler.DSPModels.Graph.GraphDSP;
import SiminStormScheduler.DSPModels.PreferencePlacementModel;
import SiminStormScheduler.NetworkModels.Graph.GraphNetwork;
import SiminStormScheduler.NetworkModels.NetworkTopologyModel;
import SiminStormScheduler.scheduler.schedulerUtility.ISchedulerUtility;
import SiminStormScheduler.scheduler.schedulerUtility.SchedulerUtility;
import org.uma.jmetal.algorithm.multiobjective.nsgaii.NSGAII;
import org.uma.jmetal.solution.integersolution.IntegerSolution;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class TestSiminScheduler {

    public static void main(String[] args) {

        //*****************************   SiminContextAwareScheduler   *****************************

        System.out.println("*** SiminContextAwareScheduler: begin scheduling");

        ISchedulerUtility schedulserUtility = new SchedulerUtility();

        int AppCount = 2;//topologies.getAllIds().size(); //= 3;//topologyset.size();

        // 1. create network and DSP graphs from profile json files addressed in storm.yaml
        System.out.println("*** TestSiminScheduler, Load DSPApplicationsFromJson and PreferencePlacement for DSP profiling from DSPRequirementSample1.json and DSPRequirementSample1.json");

        String fileNameDSPRequirement ="F:/DSP_network_profiles/";// /*this.storm_config.get(CONF_ApplicationDSPRequirementKey) !=null ? this.storm_config.get(CONF_ApplicationDSPRequirementKey).toString() :*/ "DSPRequirementSample";
        String fileNamePreferencePlacement = "F:/DSP_network_profiles/";///*this.storm_config.get(CONF_ApplicationDSPPreferenceKey) != null ? this.storm_config.get(CONF_ApplicationDSPPreferenceKey).toString() :*/ "PreferencePlacementSample";
        String fileNameNetworkTopology = "F:/DSP_network_profiles/";///*this.storm_config.get(CONF_NetworkTopologyKey)!=null ? this.storm_config.get(CONF_NetworkTopologyKey).toString() :*/ "NetworkTopologySample.json";

        //storm.yaml
        //geoAwareScheduler.in-DSPRequirementSample: "/home/simin/Desktop/SpanEdge/StormOnEdge-master/data/DSPRequirementSample1.json"
        //geoAwareScheduler.in-PreferencePlacementSample: "/home/simin/Desktop/SpanEdge/StormOnEdge-master/data/PreferencePlacementSample1.json"
        //geoAwareScheduler.in-NetworkTopologySample: "/home/simin/Desktop/SpanEdge/StormOnEdge-master/data/NetworkTopologySample.json"

        List<DSPRequirementModel> DSPRequirements = schedulserUtility.LoadDSPRequirementProfiles(AppCount, fileNameDSPRequirement);
        List<PreferencePlacementModel> PreferencePlacements = schedulserUtility.LoadPreferencePlacementProfiles(AppCount, fileNamePreferencePlacement);
        NetworkTopologyModel networkTopology = schedulserUtility.LoadNetworkTopologyProfiles(fileNameNetworkTopology);

        List<GraphDSP> DSPSet = schedulserUtility.LoadDSPApplicationsProfile(AppCount, fileNameDSPRequirement, fileNamePreferencePlacement);


        //2. Update DSP and network models From TopologySet and cluster
//        schedulserUtility.updateDPSFromTopologySet(DSPSet, topologies, cluster);
//        networkTopology = schedulserUtility.updateNetwrorkFromCluster(networkTopology, cluster);

        //3. Create DSP and Network Graph from their models
        GraphNetwork networkGraph = schedulserUtility.CreateNetworkGraph(networkTopology);


        // *** Test DSPSet
        System.out.println("*** Test DSPSet");
        for (int i = 0; i < DSPSet.size(); i++) {
            System.out.println("*** DSPSet " + i);
            System.out.println("vertex 0: " + DSPSet.get(i).getVertexes().get(0).toString());
            System.out.println("vertex 1: " + DSPSet.get(i).getVertexes().get(1).toString());
            System.out.println("edge (" + i + "," + 0 + "): " + DSPSet.get(i).getEdges().get(0).toString());
            System.out.println("edge (" + i + "," + 1 + "): " + DSPSet.get(i).getEdges().get(1).toString());
            System.out.println("edge (" + i + "," + 2 + "): " + DSPSet.get(i).getEdges().get(2).toString());
        }

        //*** Test networkGraph
        System.out.println("*** Test networkGraph");
        System.out.println("vertex 0: " + networkGraph.getVertexes().get(0).toString());
        System.out.println("vertex 1: " + networkGraph.getVertexes().get(1).toString());
        System.out.println("edge (0,0): " + networkGraph.getEdges().get(0).toString());
        System.out.println("edge (0,1): " + networkGraph.getEdges().get(1).toString());
        System.out.println("edge (1,0): " + networkGraph.getEdges().get(5).toString());


        System.out.println("*** cloud DSP and network profiles information is OK");


        //4. Create MyOptimization Problem and pass DSP graph set and Network graph to calculate objective functions for each allocaion matrix (chromozom)
        //5. Config NSGA-II algorithm parameters
        //6. run NSGA-II algorithm, get the result and store the found solutions:
        NSGAII<IntegerSolution> nsgaii = schedulserUtility.setupNSGAAlgorithm(DSPSet, networkGraph);
        List<IntegerSolution> population = nsgaii.result();


        //7. Make single result from dominate set using 1)TOPSIS or 2)Weighted sum user`s preference
        System.out.println("7. Make single result from dominate set using 1)TOPSIS or 2)Weighted sum user`s preference");
        int numberOfObjectives = 2;
        int ParetoSize = population.size();
        //7.1- Paretolist To DecisionMatrix DM[ParetoSize][numberOfObjectives]
        //double DM[][] = schedulserUtility.paretolistToDecisionMatrix(CleanPareto3, ParetoSize, numberOfObjectives);//DM[nn][numberOfObjectives]
        double DM[][] = schedulserUtility.paretolistToDecisionMatrix(population, ParetoSize, numberOfObjectives);//DM[nn][numberOfObjectives]
        //7.2- Weighting
        double v[][] = schedulserUtility.entropy(DM, ParetoSize, numberOfObjectives);
        //7.3- MADM : TOPSIS
        int indxBestSolution = schedulserUtility.TOPSIS(v, ParetoSize, numberOfObjectives);
        //7.4- Print the BestSolution
        IntegerSolution bestSolution = population.get(indxBestSolution);


        //8. convert allocation 3-D matrix to  StormAssignment
        System.out.println("8. convert allocation 3-D matrix to  Storm Task to Node Assignment");
        //mapAllocationMatrixToStormAssignment();
        int NodeCount = networkGraph.getVertexes().size();
        //calculate TaskMaxCount over all applications
        Integer tasksize[] = new Integer[AppCount];
        for (int b = 0; b < AppCount; b++) {
            tasksize[b] = DSPSet.get(b).getVertexes().size();
        }
        int TaskMaxCount = 0;
        if (tasksize != null && tasksize.length > 0)
         TaskMaxCount = Collections.max(Arrays.asList(tasksize));//TODO: max(dbset.app.vertex.size)

        boolean[][][] A = schedulserUtility.convertChromozomTo3DMatrix (bestSolution, DSPSet, AppCount, NodeCount, TaskMaxCount);

        //*****************************


    }
}
