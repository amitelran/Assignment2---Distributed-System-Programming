package com.amazonaws.samples;

import java.io.File;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.Vector;

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.ec2.model.InstanceType;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduce;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClientBuilder;
import com.amazonaws.services.elasticmapreduce.model.HadoopJarStepConfig;
import com.amazonaws.services.elasticmapreduce.model.JobFlowInstancesConfig;
import com.amazonaws.services.elasticmapreduce.model.PlacementType;
import com.amazonaws.services.elasticmapreduce.model.RunJobFlowRequest;
import com.amazonaws.services.elasticmapreduce.model.RunJobFlowResult;
import com.amazonaws.services.elasticmapreduce.model.StepConfig;

import corpusData.Words;

public class Main {

	public static void main(String[] args) throws UnsupportedEncodingException, IOException {
	
		//final int N = 10;																			//  top N similar tweets (argument of the program, should be set to 10)
		//DefaultAWSCredentialsProviderChain credentials = new DefaultAWSCredentialsProviderChain();			// Search and get credentials file in system
		//AmazonElasticMapReduce mapReduce = AmazonElasticMapReduceClientBuilder.standard().withCredentials(credentials).withRegion(Regions.US_EAST_1).build();
		 
		// Set stop words vector from file
		Vector<String> stopWords = new Vector<String>();
		File file = new File("C:\\Users\\Amir\\Desktop\\stop_words.txt");
		Words.readStopWords(file, stopWords);
		
		/*
		HadoopJarStepConfig hadoopJarStep = new HadoopJarStepConfig()
		    .withJar("s3n://yourbucket/yourfile.jar") // This should be a full map reduce application.
		    .withMainClass("some.pack.MainClass")
		    .withArgs("s3n://yourbucket/input/", "s3n://yourbucket/output/");
		 
		
		StepConfig stepConfig = new StepConfig()
		    .withName("stepname")
		    .withHadoopJarStep(hadoopJarStep)
		    .withActionOnFailure("TERMINATE_JOB_FLOW");
		 
		
		JobFlowInstancesConfig instances = new JobFlowInstancesConfig()
		    .withInstanceCount(2)														// Number of instances to run
		    .withMasterInstanceType(InstanceType.M1Small.toString())
		    .withSlaveInstanceType(InstanceType.M1Small.toString())
		    .withHadoopVersion("2.2.0")													// Hadoop version
		    .withEc2KeyName("yourkey")													// EC2 key name
		    .withKeepJobFlowAliveWhenNoSteps(false)										// Set job to terminate when finishing all steps
		    .withPlacement(new PlacementType("us-east-1a"));							// Set region as "us-east-1"
		 
		
		// Set job flow request (the job to run)
		RunJobFlowRequest runFlowRequest = new RunJobFlowRequest()
		    .withName("jobname")														// Name of the job flow
		    .withInstances(instances)													// A specification of the number and type of Amazon EC2 instances.
		    .withSteps(stepConfig)														// A list of steps to run.
		    .withLogUri("s3n://yourbucket/logs/");										// The location in Amazon S3 to write the log files of the job flow.
		 
		
		// RunJobFlow creates and starts running a new cluster (job flow).
		RunJobFlowResult runJobFlowResult = mapReduce.runJobFlow(runFlowRequest);		
		String jobFlowId = runJobFlowResult.getJobFlowId();								// The job flow ID
		System.out.println("Ran job flow with id: " + jobFlowId);
	*/
	}

}
