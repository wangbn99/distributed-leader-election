package com.comtwins.leaderelection;

public interface LeaderElection {
	
	LeaderService getLeaderService();

	void setLeaderService(LeaderService leaderService);
	
	void stopLeader();

	void startLeaderElection();

}