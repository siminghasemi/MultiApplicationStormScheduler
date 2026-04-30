package SiminStormScheduler.scheduler;

import SiminStormScheduler.DSPModels.DSPRequirementModel;
import SiminStormScheduler.DSPModels.Graph.GraphDSP;
import SiminStormScheduler.DSPModels.PreferencePlacementModel;
import SiminStormScheduler.NetworkModels.Graph.GraphNetwork;
import SiminStormScheduler.NetworkModels.NetworkTopologyModel;
//import SiminStormScheduler.grouping.topology.GlobalTaskGroup;
//import SiminStormScheduler.grouping.topology.LocalTaskGroup;
//import SiminStormScheduler.grouping.topology.TaskGroup;
import SiminStormScheduler.scheduler.schedulerUtility.ISchedulerUtility;
import SiminStormScheduler.scheduler.schedulerUtility.SchedulerUtility;
//import SiminStormScheduler.state.CloudState.CloudsInfo;
//import SiminStormScheduler.state.CloudState.FileBasedCloudsInfo;
//import SiminStormScheduler.state.SourceState.FileSourceInfo;
//import SiminStormScheduler.state.SourceState.SourceInfo;
//import SiminStormScheduler.state.ZGState.FileBasedZGConnector;
//import SiminStormScheduler.state.ZGState.ZGConnector;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.metric.StormMetricsRegistry;
import org.apache.storm.scheduler.*;
import org.json.simple.parser.JSONParser;
import org.mortbay.util.MultiMap;
import org.uma.jmetal.algorithm.multiobjective.nsgaii.NSGAII;
import org.uma.jmetal.solution.integersolution.IntegerSolution;

import java.io.File;
import java.util.*;

import static org.apache.storm.scheduler.EvenScheduler.getAliveAssignedWorkerSlotExecutors;

@SuppressWarnings("Duplicates")
public class SiminContextAwareScheduler implements IScheduler {

    private static final Log LOG = LogFactory.getLog(SiminContextAwareScheduler.class);
    // application and network profile file names
    final String CONF_ApplicationDSPRequirementKey = "geoAwareSiminScheduler.in-DSPRequirementSample";//DSPRequirementSample1.json
    final String CONF_ApplicationDSPPreferenceKey = "geoAwareSiminScheduler.in-PreferencePlacementSample";//PreferencePlacementSample1.json
    final String CONF_NetworkTopologyKey = "geoAwareSiminScheduler.in-NetworkTopologySample";//NetworkTopologySample.json

    Random rand = new Random(System.currentTimeMillis());
    JSONParser parser = new JSONParser();
    ISchedulerUtility schedulserUtility;
    Map storm_config;

    final String ackerBolt = "__acker";
    final String CONF_sourceCloudKey = "geoAwareScheduler.in-SourceInfo";
    final String CONF_cloudLocatorKey = "geoAwareScheduler.in-CloudInfo";
    final String CONF_schedulerResult = "geoAwareScheduler.out-SchedulerResult";
    //final String CONF_ZoneGroupingInput = "geoScheduler.out-ZoneGrouping";

    //String taskGroupListFile = "/home/kend/fromSICSCloud/Scheduler-GroupList.txt";
    //String schedulerResultFile = "/home/kend/SchedulerResult.csv";
    //String pairSupervisorTaskFile = "/home/kend/fromSICSCloud/PairSupervisorTasks.txt";
    //@SuppressWarnings("rawtypes")
    public void prepare(Map conf) {//the conf is the storm.yaml file as a hashmap and includes all the configuration as key:value 
        //the conf is the storm.yaml file as a hashmap and includes all the configuration as key:value
        storm_config = conf;
        //Retrieve data from storm.yaml config file
        //geoAwareScheduler.in-SourceInfo: "/home/simin/Desktop/SpanEdge/StormOnEdge-master/data/Scheduler-SpoutCloudsPair.txt"
        //geoAwareScheduler.in-CloudInfo: "/home/simin/Desktop/SpanEdge/StormOnEdge-master/data/Scheduler-LatencyMatrix.txt"
        //geoAwareScheduler.out-ZGConnector: "/home/simin/Desktop/SpanEdge/StormOnEdge-master/data/PairSupervisorTasks.txt"
    }


    //@SuppressWarnings({"unchecked", "ConstantConditions"})
    @Override
    public void schedule(Topologies topologies, Cluster cluster) {
        //*****************************   SiminContextAwareScheduler   *****************************

        System.out.println("*** SiminContextAwareScheduler: begin scheduling");
        //LOG.info("SiminContextAwareScheduler: begin scheduling");

        //********** catch up executors and worker slot ***********

        int AppCount = topologies.getAllIds().size(); //= 3;//topologyset.size();
        System.out.println("*** AppCount: topologies.getAllIds().size(): " + AppCount);//1
        int NodeCount;
        //print topolgy list specification
        System.out.println("*** cluster.needsSchedulingTopologies().size() " + cluster.needsSchedulingTopologies().size());//1
        for (TopologyDetails topology : cluster.needsSchedulingTopologies()) {
            System.out.println("*** topology.Id: " + topology.getId());//"WordCountSimin-1-1721461846"
            System.out.println("*** topology.name: " + topology.getName());// "WordCountSimin"
            System.out.println("*** topology.numberOfOperator: " + topology.getExecutors().size()); // 28

            StormTopology st = topology.getTopology();
            //System.out.println("*** topology.getTopology: " + st.toString()); // 28

            Set<ExecutorDetails> allExecutors = topology.getExecutors();
            Iterator<ExecutorDetails> executorIterator = allExecutors.iterator();
            while (executorIterator.hasNext()) {
                ExecutorDetails executor = executorIterator.next();
                //System.out.println("*** executor: " + executor.toString());
            }
            Map<WorkerSlot, List<ExecutorDetails>> aliveAssigned = getAliveAssignedWorkerSlotExecutors(cluster, topology.getId());


        }
//        List<WorkerSlot> availableSlots = cluster.getAvailableSlots();
//        Set<ExecutorDetails> allExecutors = topologies.getExecutors();
//        Map<WorkerSlot, List<ExecutorDetails>> aliveAssigned = getAliveAssignedWorkerSlotExecutors(cluster, topology.getId());
//        int totalSlotsToUse = Math.min(topologies.getNumWorkers(), availableSlots.size() + aliveAssigned.size());

        //print the worker list
        System.out.println("*** print the worker list, pay attention to their port numbers.");
        //LOG.info("print the worker list, pay attention to their port numbers.");
        List<SupervisorDetails> supervisors = new ArrayList<SupervisorDetails>(cluster.getSupervisors().values());
        int i = 0;
        for (SupervisorDetails supervisor : supervisors) {
            //supervisor[0]: Host:ubuntu, Id:cac27e64-3a72-43b5-8b38-bbc0f55ca305-127.0.1.1,
            // AllPorts:[6700, 6701, 6702, 6703], TotalMemory:4096.0, TotalCpu:400.0
            System.out.println("*** supervisor[" + i + "]: Host:" + supervisor.getHost()
                    + ", Id:" + supervisor.getId()
                    + ", AllPorts:" + supervisor.getAllPorts().toString()
                    + ", TotalMemory:" + supervisor.getTotalMemory()
                    + ", TotalCpu:" + supervisor.getTotalCpu());

            Map<String, Object> metadata = (Map<String, Object>) supervisor.getSchedulerMeta();
            //simin explains: in storm.yaml of each supervisor, it add this metadata:
            //## On supervisor node: Every nodes that run Supervisor instances need to put information about their cloud location
            //supervisor.scheduler.meta:
            //name: "SUPERVISOR_1"
            //cloud-name: "CLOUD_A"
            //speed: "1.5"
            if (metadata.get("speed") != null) {//cloud-name
                System.out.println("*** metadata.get(\"speed\"): " + metadata.get("speed").toString());
            }
            List<WorkerSlot> workerSlots = cluster.getAvailableSlots(supervisor);
            System.out.println("*** Available Workers for supervisor[" + i + "]: " + workerSlots.size() + "\n");//4
            Iterator<WorkerSlot> workerIterator = workerSlots.iterator();
            while (workerIterator.hasNext()) {
                WorkerSlot workerSlot = workerIterator.next();
                System.out.println("*** NodeId: " + workerSlot.getNodeId() + ", port: " + workerSlot.getPort());
                //NodeId: cac27e64-3a72-43b5-8b38-bbc0f55ca305-127.0.1.1, port: 6700
            }
            i++;
        }

        //********** catch up executors and worker slot ***********

        schedulserUtility = new SchedulerUtility();
        // 1. create network and DSP graphs from profile json files addressed in storm.yaml
        System.out.println("Load DSPApplicationsFromJson and PreferencePlacement for DSP profiling from DSPRequirementSample1.json and DSPRequirementSample1.json");
        String fileNameDSPRequirement = this.storm_config.get(CONF_ApplicationDSPRequirementKey) != null ? this.storm_config.get(CONF_ApplicationDSPRequirementKey).toString() : "DSPRequirementSample";
        String fileNamePreferencePlacement = this.storm_config.get(CONF_ApplicationDSPPreferenceKey) != null ? this.storm_config.get(CONF_ApplicationDSPPreferenceKey).toString() : "PreferencePlacementSample";
        String fileNameNetworkTopology = this.storm_config.get(CONF_NetworkTopologyKey) != null ? this.storm_config.get(CONF_NetworkTopologyKey).toString() : "NetworkTopologySample.json";

        //storm.yaml
        //geoAwareScheduler.in-DSPRequirementSample: "/home/simin/Desktop/SpanEdge/StormOnEdge-master/data/DSPRequirementSample1.json"
        //geoAwareScheduler.in-PreferencePlacementSample: "/home/simin/Desktop/SpanEdge/StormOnEdge-master/data/PreferencePlacementSample1.json"
        //geoAwareScheduler.in-NetworkTopologySample: "/home/simin/Desktop/SpanEdge/StormOnEdge-master/data/NetworkTopologySample.json"

        List<DSPRequirementModel> DSPRequirements = schedulserUtility.LoadDSPRequirementProfiles(AppCount, fileNameDSPRequirement);
        List<PreferencePlacementModel> PreferencePlacements = schedulserUtility.LoadPreferencePlacementProfiles(AppCount, fileNamePreferencePlacement);

        NetworkTopologyModel networkTopology = schedulserUtility.LoadNetworkTopologyProfiles(fileNameNetworkTopology);

        List<GraphDSP> DSPSet = schedulserUtility.LoadDSPApplicationsProfile(AppCount, fileNameDSPRequirement, fileNamePreferencePlacement);

        //2. Update DSP and network models From TopologySet and cluster
        schedulserUtility.updateDPSFromTopologySet(DSPSet, topologies, cluster);
        networkTopology = schedulserUtility.updateNetwrorkFromCluster(networkTopology, cluster);

        //3. Create DSP and Network Graph from their models
        GraphNetwork networkGraph = schedulserUtility.CreateNetworkGraph(networkTopology);


        NodeCount = cluster.getSupervisors().size();
        System.out.println("*** AppCount: networkGraph.getVertexes().size(): " + networkGraph.getVertexes().size());
        System.out.println("*** AppCount: cluster.getSupervisors().size(): " + cluster.getSupervisors().size());

        System.out.println("cloud DSP and network profiles information is OK");

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
        if (DSPSet != null) {
            NodeCount = networkGraph.getVertexes().size();
            //calculate TaskMaxCount over all applications
            Integer tasksize[] = new Integer[AppCount];
            if (tasksize != null && tasksize.length > 0)
                for (int b = 0; b < AppCount; b++) {
                    if (DSPSet != null && DSPSet.get(b) != null && DSPSet.get(b).getVertexes() != null)
                        tasksize[b] = DSPSet.get(b).getVertexes().size();
                }
            int TaskMaxCount = 0;
            if (tasksize != null && tasksize.length > 0)
                TaskMaxCount = Collections.max(Arrays.asList(tasksize));//TODO: max(dbset.app.vertex.size)

            boolean[][][] AllocationTaskMatrix = schedulserUtility.convertChromozomTo3DMatrix(bestSolution, DSPSet, AppCount, NodeCount, TaskMaxCount);


            //*************** storm executor to worker_slot assingment
            System.out.println("9. cstorm executor to worker_slot assingment");
            for (int appIndex = 0; appIndex < AppCount; appIndex++) {//for every appIndex

                GraphDSP DSPGraph = DSPSet.get(appIndex);
                TopologyDetails topology = schedulserUtility.findTopologyIdByAppName(cluster.needsSchedulingTopologies(), DSPSet.get(appIndex));

                if (topology != null) {

                    for (int nodeIndex = 0; nodeIndex < NodeCount; nodeIndex++) {

                        SupervisorDetails supervisor = schedulserUtility.findSupervisorByNodeIndex(supervisors, networkGraph.getNetworkTopology(), nodeIndex);
                        if (supervisor != null) {
                            MultiMap executorWorkerMap = new MultiMap();
                            List<WorkerSlot> workerSlotsofNodeIndex = cluster.getAvailableSlots(supervisor);

                            List<ExecutorDetails> executorListAssignedToNodeIndex = schedulserUtility.findExecutorsAssignedToNodeIndex(topology, AllocationTaskMatrix, appIndex, nodeIndex, TaskMaxCount);

                            //determine assingment of executors of this node to its worker_slots (port number ex. 6700, 6701, 6702)
                            //deployExecutorToWorkers(workerslotList, executors, executorWorkerMap);
                            deployExecutorToWorkers(workerSlotsofNodeIndex, executorListAssignedToNodeIndex, executorWorkerMap);

                            //for each worker slot of this node, assing its executors in storm cluster
                            for (Object ws : executorWorkerMap.keySet()) {
                                List<ExecutorDetails> executordetailList = (List<ExecutorDetails>) executorWorkerMap.getValues(ws);
                                WorkerSlot workerSlot = (WorkerSlot) ws;

                                //cluster.assign(wslot, topology.getId(), edetails);
                                cluster.assign(workerSlot, topology.getId(), executordetailList);

                                System.out.println("*** We assigned executors:" + executorWorkerMap.getValues(ws) + " to slot: [" + workerSlot.getNodeId() + ", " + workerSlot.getPort() + "], for supervisor: " + supervisor.toString());
                                //schedulerResultStringBuilder.append(executorWorkerMap.getValues(ws) + " to slot: [" + wslot.getNodeId() + ", " + wslot.getPort() + "]\n");
                            }

                        }//if (supervisor != null)
                    }//for ( nodeIndex < NodeCount; )
                }//if (!topologyId.isEmpty())
            }//for ( appIndex < AppCount; )

        }

        //*****************************


        // let system's even StormOnEdge.scheduler handle the rest scheduling work
        // you can also use your own other StormOnEdge.scheduler here, this is what
        // makes storm's StormOnEdge.scheduler composable.
        new EvenScheduler().schedule(topologies, cluster);
    }


    private boolean checkFileAvailability(String path) {
        File file = new File(path);
        return file.exists();
    }

    private void deployExecutorToWorkers(List<WorkerSlot> cloudWorkers, List<ExecutorDetails> executors, MultiMap executorWorkerMap) {
        Iterator<WorkerSlot> workerIterator = cloudWorkers.iterator();
        Iterator<ExecutorDetails> executorIterator = executors.iterator();

        //if executors >= workers, do simple round robin
        //for all executors A to all supervisors B
        if (executors.size() >= cloudWorkers.size()) {
            while (executorIterator.hasNext() && workerIterator.hasNext()) {
                WorkerSlot w = workerIterator.next();
                ExecutorDetails ed = executorIterator.next();
                executorWorkerMap.add(w, ed);

                //Reset to first worker again
                if (!workerIterator.hasNext()) {
                    workerIterator = cloudWorkers.iterator();
                }
            }
        } //if workers > executors, choose randomly
        //for all executors A to all supervisors B
        else {
            while (executorIterator.hasNext() && !cloudWorkers.isEmpty()) {
                WorkerSlot w = cloudWorkers.get(rand.nextInt(cloudWorkers.size()));
                ExecutorDetails ed = executorIterator.next();
                executorWorkerMap.add(w, ed);
            }
        }
    }

    //@SuppressWarnings("rawtypes")
    @Override
    public void prepare(Map<String, Object> map, StormMetricsRegistry smr) {
        //throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
        //IScheduler.super.prepare(map, smr);
        //Retrieve data from storm.yaml config file
        storm_config = map;
        LOG.info("*** GeoAwareGroupScheduler: prepare");
    }

    @Override
    public Map config() {
        //throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
        //This function returns the scheduler’s configuration.
        LOG.info("GeoAwareGroupScheduler: config");
        return storm_config;
    }

    @Override
    public void cleanup() {
        IScheduler.super.cleanup(); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void nodeAssignmentSent(String node, boolean successful) {
        IScheduler.super.nodeAssignmentSent(node, successful); //To change body of generated methods, choose Tools | Templates.
    }

}
