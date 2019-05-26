import unittest
import requests
import time
import os

######################## initialize variables ################################################
subnetName = "assignment3-net"
subnetAddress = "10.10.0.0/16"

replica1Name = "replica1"
replica2Name = "replica2"
replica3Name = "replica3"

ipAddrReplica1 = "10.10.0.2"
ipAddrReplica2 = "10.10.0.3"
ipAddrReplica3 = "10.10.0.4"

hostPortReplica1 = "8082"
hostPortReplica2 = "8083"
hostPortReplica3 = "8084"
exposedPortNumber = "8080"

viewEnvironment = "10.10.0.2:8080,10.10.0.3:8080,10.10.0.4:8080"

############################### Docker Linux Commands ###########################################################
def removeSubnet(subnetName):
    command = "docker network rm " + subnetName + " &> /dev/null"
    os.system(command)

def createSubnet(subnetAddress, subnetName):
    command  = "docker network create --subnet=" + subnetAddress + " " + subnetName + " &> /dev/null"
    os.system(command)

def buildDockerImage():
    command = "docker build -t assignment3-img ."
    os.system(command)

def runReplicaInstance(hostPortNumber, ipAddress, subnetName, instanceName, viewEnv):
    command = "docker run -p " + hostPortNumber + ":8080 --net=" + subnetName + " --ip=" + ipAddress + " --name=" + instanceName + " -e SOCKET_ADDRESS=" + ipAddress + ":8080 -e VIEW=" + viewEnv + " assignment3-img" + " &> /dev/null"
    os.system(command)

def stopAndRemoveInstance(instanceName):
    stopCommand = "docker stop " + instanceName + " &> /dev/null"
    removeCommand = "docker rm " + instanceName + " &> /dev/null"
    os.system(stopCommand)
    time.sleep(1)
    os.system(removeCommand)

################################# Unit Test Class ############################################################
class TestHW3(unittest.TestCase): 
    ######################## Functions to send the required requests ##########################################
    def send_all_kvs_requests(self, baseUrl):
        return
    
    def send_all_view_requests(self, baseUrl):
        return

    def send_kvs_requests_with_replica_stopped(self, baseUrl):
        return

    def send_view_requests_with_replica_stopped(self, baseUrl):
        return

    def send_requests_to_replica_ports(self, replica1Url, replica2Url, replica3Url):
        return

    def send_kvs_versions_out_of_order(self, baseUrl):
        return

    def send_kvs_versions_out_of_order_with_replica_stopped(self, baseUrl):
        return

    ######################## Build docker image and create subnet ################################
    # build docker image
    buildDockerImage()

    # stop the containers using the subnet
    stopAndRemoveInstance(replica1Name)
    stopAndRemoveInstance(replica2Name)
    stopAndRemoveInstance(replica3Name)

    # remove the subnet possibly created from the previous run
    removeSubnet(subnetName)

    # create subnet
    createSubnet(subnetAddress, subnetName)


    ########################## Run tests #######################################################
    def test_a_all_running_kvs_requests(self):
        print("\nRunning all replicas and sending key-value store requests to all")
        
        # stop and remove containers
        print("\tStopping and removing containers from previous run ...")
        stopAndRemoveInstance(replica1Name)
        stopAndRemoveInstance(replica2Name)
        stopAndRemoveInstance(replica3Name)

        # run replicas
        print("\tRunning replicas ...")
        runReplicaInstance(hostPortReplica1, ipAddrReplica1, subnetName, replica1Name, viewEnvironment)
        runReplicaInstance(hostPortReplica2, ipAddrReplica2, subnetName, replica2Name, viewEnvironment)
        runReplicaInstance(hostPortReplica3, ipAddrReplica3, subnetName, replica3Name, viewEnvironment)

        time.sleep(10)
        # Should we be testing the replica's port # as well? Or just the exposed?
        baseUrl = "http://localhost:" + exposedPortNumber

        print("\Sending requests ...")
        self.send_all_kvs_requests(baseUrl)


    def test_b_all_running_view_requests(self):
        print("\nRunning all replicas and sending key-value store requests to all")
        
        # stop and remove containers
        print("\tStopping and removing containers from previous run ...")
        stopAndRemoveInstance(replica1Name)
        stopAndRemoveInstance(replica2Name)
        stopAndRemoveInstance(replica3Name)

        # run replicas
        print("\tRunning replicas ...")
        runReplicaInstance(hostPortReplica1, ipAddrReplica1, subnetName, replica1Name, viewEnvironment)
        runReplicaInstance(hostPortReplica2, ipAddrReplica2, subnetName, replica2Name, viewEnvironment)
        runReplicaInstance(hostPortReplica3, ipAddrReplica3, subnetName, replica3Name, viewEnvironment)

        time.sleep(10)
        # Should we be testing the replica's port # as well? Or just the exposed?
        baseUrl = "http://localhost:" + exposedPortNumber

        print("\tSending requests ...")
        self.send_all_view_requests(baseUrl)

    def test_c_replica_stopped_kvs_requests(self):
        print("\nRunning all replicas and sending key-value store requests to all")
        
        # stop and remove containers
        print("\tStopping and removing containers from previous run ...")
        stopAndRemoveInstance(replica1Name)
        stopAndRemoveInstance(replica2Name)
        stopAndRemoveInstance(replica3Name)

        # run replicas
        print("\tRunning replicas ...")
        runReplicaInstance(hostPortReplica1, ipAddrReplica1, subnetName, replica1Name, viewEnvironment)
        runReplicaInstance(hostPortReplica2, ipAddrReplica2, subnetName, replica2Name, viewEnvironment)
        runReplicaInstance(hostPortReplica3, ipAddrReplica3, subnetName, replica3Name, viewEnvironment)

        time.sleep(10)
        baseUrl = "http://localhost:" + exposedPortNumber

        print("\tSending requests ...")
        self.send_kvs_requests_with_replica_stopped(baseUrl)

    def test_d_replica_stopped_view_requests(self):
        print("\nRunning all replicas and sending key-value store requests to all")
        
        # stop and remove containers
        print("\tStopping and removing containers from previous run ...")
        stopAndRemoveInstance(replica1Name)
        stopAndRemoveInstance(replica2Name)
        stopAndRemoveInstance(replica3Name)

        # run replicas
        print("\tRunning replicas ...")
        runReplicaInstance(hostPortReplica1, ipAddrReplica1, subnetName, replica1Name, viewEnvironment)
        runReplicaInstance(hostPortReplica2, ipAddrReplica2, subnetName, replica2Name, viewEnvironment)
        runReplicaInstance(hostPortReplica3, ipAddrReplica3, subnetName, replica3Name, viewEnvironment)

        time.sleep(10)
        baseUrl = "http://localhost:" + exposedPortNumber

        print("\tSending requests ...")
        self.send_kvs_requests_with_replica_stopped(baseUrl)

    def test_e_send_requests_to_replica_ports(self):
        print("\nRunning all replicas and sending key-value store requests to all")
        
        # stop and remove containers
        print("\tStopping and removing containers from previous run ...")
        stopAndRemoveInstance(replica1Name)
        stopAndRemoveInstance(replica2Name)
        stopAndRemoveInstance(replica3Name)

        # run replicas
        print("\tRunning replicas ...")
        runReplicaInstance(hostPortReplica1, ipAddrReplica1, subnetName, replica1Name, viewEnvironment)
        runReplicaInstance(hostPortReplica2, ipAddrReplica2, subnetName, replica2Name, viewEnvironment)
        runReplicaInstance(hostPortReplica3, ipAddrReplica3, subnetName, replica3Name, viewEnvironment)

        time.sleep(10)
        replica1Url = "http://localhost:" + hostPortReplica1
        replica2Url = "http://localhost:" + hostPortReplica2
        replica3Url = "http://localhost:" + hostPortReplica3

        print("\tSending requests ...")
        self.send_requests_to_replica_ports(replica1Url, replica2Url, replica3Url)
    
    def test_f_send_out_of_order_kvs_version_requests(self):
        print("\nRunning all replicas and sending key-value store requests to all")
        
        # stop and remove containers
        print("\tStopping and removing containers from previous run ...")
        stopAndRemoveInstance(replica1Name)
        stopAndRemoveInstance(replica2Name)
        stopAndRemoveInstance(replica3Name)

        # run replicas
        print("\tRunning replicas ...")
        runReplicaInstance(hostPortReplica1, ipAddrReplica1, subnetName, replica1Name, viewEnvironment)
        runReplicaInstance(hostPortReplica2, ipAddrReplica2, subnetName, replica2Name, viewEnvironment)
        runReplicaInstance(hostPortReplica3, ipAddrReplica3, subnetName, replica3Name, viewEnvironment)

        time.sleep(10)
        baseUrl = "http://localhost:" + exposedPortNumber

        self.send_kvs_versions_out_of_order(baseUrl)
    
    def test_g_send_versions_with_replica_stopped(self):
        print("\nRunning all replicas and sending key-value store requests to all")
        
        # stop and remove containers
        print("\tStopping and removing containers from previous run ...")
        stopAndRemoveInstance(replica1Name)
        stopAndRemoveInstance(replica2Name)
        stopAndRemoveInstance(replica3Name)

        # run replicas
        print("\tRunning replicas ...")
        runReplicaInstance(hostPortReplica1, ipAddrReplica1, subnetName, replica1Name, viewEnvironment)
        runReplicaInstance(hostPortReplica2, ipAddrReplica2, subnetName, replica2Name, viewEnvironment)
        runReplicaInstance(hostPortReplica3, ipAddrReplica3, subnetName, replica3Name, viewEnvironment)

        time.sleep(10)
        baseUrl = "http://localhost:" + exposedPortNumber

        self.send_kvs_versions_out_of_order_with_replica_stopped(baseUrl)

if __name__ == '__main__':
    unittest.main()