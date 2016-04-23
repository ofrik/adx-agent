package gso;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import java.util.Random;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.commons.lang3.text.WordUtils;

import se.sics.isl.transport.Transportable;
import se.sics.tasim.aw.Agent;
import se.sics.tasim.aw.Message;
import se.sics.tasim.props.SimulationStatus;
import se.sics.tasim.props.StartInfo;
import tau.tac.adx.ads.properties.AdType;
import tau.tac.adx.demand.CampaignStats;
import tau.tac.adx.devices.Device;
import tau.tac.adx.props.AdxBidBundle;
import tau.tac.adx.props.AdxQuery;
import tau.tac.adx.props.PublisherCatalog;
import tau.tac.adx.props.PublisherCatalogEntry;
import tau.tac.adx.props.ReservePriceInfo;
import tau.tac.adx.report.adn.AdNetworkKey;
import tau.tac.adx.report.adn.AdNetworkReport;
import tau.tac.adx.report.adn.AdNetworkReportEntry;
import tau.tac.adx.report.adn.MarketSegment;
import tau.tac.adx.report.demand.AdNetBidMessage;
import tau.tac.adx.report.demand.AdNetworkDailyNotification;
import tau.tac.adx.report.demand.CampaignOpportunityMessage;
import tau.tac.adx.report.demand.CampaignReport;
import tau.tac.adx.report.demand.CampaignReportKey;
import tau.tac.adx.report.demand.InitialCampaignMessage;
import tau.tac.adx.report.demand.campaign.auction.CampaignAuctionReport;
import tau.tac.adx.report.publisher.AdxPublisherReport;
import tau.tac.adx.report.publisher.AdxPublisherReportEntry;
import edu.umich.eecs.tac.props.Ad;
import edu.umich.eecs.tac.props.BankStatus;

/**
 * @author Mariano Schain Test plug-in
 */
public class GSOAgent extends Agent {

	private final Logger log = Logger.getLogger(GSOAgent.class.getName());

	/*
	 * Basic simulation information. An agent should receive the {@link
	 * StartInfo} at the beginning of the game or during recovery.
	 */
	@SuppressWarnings("unused")
	private StartInfo startInfo;

	/**
	 * Messages received:
	 * <p/>
	 * We keep all the {@link CampaignReport campaign reports} delivered to the
	 * agent. We also keep the initialization messages {@link PublisherCatalog}
	 * and {@link InitialCampaignMessage} and the most recent messages and
	 * reports {@link CampaignOpportunityMessage}, {@link CampaignReport}, and
	 * {@link AdNetworkDailyNotification}.
	 */
	private final Queue<CampaignReport> campaignReports;
	private PublisherCatalog publisherCatalog;
	private InitialCampaignMessage initialCampaignMessage;
	private AdNetworkDailyNotification adNetworkDailyNotification;

	/*
	 * The addresses of server entities to which the agent should send the daily
	 * bids data
	 */
	private String demandAgentAddress;
	private String adxAgentAddress;

	/*
	 * we maintain a list of queries - each characterized by the web site (the
	 * publisher), the device type, the ad type, and the user market segment
	 */
	private AdxQuery[] queries;

	/**
	 * Information regarding the latest campaign opportunity announced
	 */
	private CampaignData pendingCampaign;

	/**
	 * We maintain a collection (mapped by the campaign id) of the campaigns won
	 * by our agent.
	 */
	private Map<Integer, CampaignData> myCampaigns;

	/*
	 * the bidBundle to be sent daily to the AdX
	 */
	private AdxBidBundle bidBundle;

	/*
	 * The current bid level for the user classification service
	 */
	private double ucsBid;

	/*
	 * The targeted service level for the user classification service
	 */
	private double ucsTargetLevel;

	/*
	 * current day of simulation
	 */
	private int day;
	private String[] publisherNames;
	private CampaignData currCampaign;
	private Map<String, ArrayList<CampaignData>> othersCampaigns;
	private double lastQualityScore;
	private int winsInARow;
	private Map<Long,Double> historyFactor;
	private Map<Set<MarketSegment>,Integer> segmentProb = new HashMap<Set<MarketSegment>,Integer>();
	private double USCSum;
	private int USCCount;
	public GSOAgent() {
		campaignReports = new LinkedList<CampaignReport>();
		segmentProb.put(new HashSet<MarketSegment>(Arrays.asList(MarketSegment.YOUNG)), 4589);
		segmentProb.put(new HashSet<MarketSegment>(Arrays.asList(MarketSegment.OLD)), 5411);
		segmentProb.put(new HashSet<MarketSegment>(Arrays.asList(MarketSegment.MALE)), 4956);
		segmentProb.put(new HashSet<MarketSegment>(Arrays.asList(MarketSegment.FEMALE)), 5044);
		segmentProb.put(new HashSet<MarketSegment>(Arrays.asList(MarketSegment.HIGH_INCOME)), 1988);
		segmentProb.put(new HashSet<MarketSegment>(Arrays.asList(MarketSegment.LOW_INCOME)), 8012);
		
		segmentProb.put(new HashSet<MarketSegment>(Arrays.asList(MarketSegment.MALE,MarketSegment.LOW_INCOME)), 3631);
		segmentProb.put(new HashSet<MarketSegment>(Arrays.asList(MarketSegment.FEMALE,MarketSegment.LOW_INCOME)), 4381);
		segmentProb.put(new HashSet<MarketSegment>(Arrays.asList(MarketSegment.MALE,MarketSegment.HIGH_INCOME)), 1325);
		segmentProb.put(new HashSet<MarketSegment>(Arrays.asList(MarketSegment.FEMALE,MarketSegment.HIGH_INCOME)), 663);
		segmentProb.put(new HashSet<MarketSegment>(Arrays.asList(MarketSegment.YOUNG,MarketSegment.LOW_INCOME)), 3816);
		segmentProb.put(new HashSet<MarketSegment>(Arrays.asList(MarketSegment.OLD,MarketSegment.LOW_INCOME)), 4196);
		segmentProb.put(new HashSet<MarketSegment>(Arrays.asList(MarketSegment.MALE,MarketSegment.HIGH_INCOME)), 773);
		segmentProb.put(new HashSet<MarketSegment>(Arrays.asList(MarketSegment.OLD,MarketSegment.HIGH_INCOME)), 1215);
		segmentProb.put(new HashSet<MarketSegment>(Arrays.asList(MarketSegment.MALE,MarketSegment.YOUNG)), 2353);
		segmentProb.put(new HashSet<MarketSegment>(Arrays.asList(MarketSegment.MALE,MarketSegment.OLD)), 2603);
		segmentProb.put(new HashSet<MarketSegment>(Arrays.asList(MarketSegment.FEMALE,MarketSegment.YOUNG)), 2236);
		segmentProb.put(new HashSet<MarketSegment>(Arrays.asList(MarketSegment.FEMALE,MarketSegment.OLD)), 2808);
		
		segmentProb.put(new HashSet<MarketSegment>(Arrays.asList(MarketSegment.MALE,MarketSegment.LOW_INCOME,MarketSegment.YOUNG)), 1836);
		segmentProb.put(new HashSet<MarketSegment>(Arrays.asList(MarketSegment.MALE,MarketSegment.LOW_INCOME,MarketSegment.OLD)), 1795);
		segmentProb.put(new HashSet<MarketSegment>(Arrays.asList(MarketSegment.FEMALE,MarketSegment.LOW_INCOME,MarketSegment.YOUNG)), 1980);
		segmentProb.put(new HashSet<MarketSegment>(Arrays.asList(MarketSegment.FEMALE,MarketSegment.LOW_INCOME,MarketSegment.OLD)), 2401);
		
		segmentProb.put(new HashSet<MarketSegment>(Arrays.asList(MarketSegment.MALE,MarketSegment.HIGH_INCOME,MarketSegment.YOUNG)), 517);
		segmentProb.put(new HashSet<MarketSegment>(Arrays.asList(MarketSegment.MALE,MarketSegment.HIGH_INCOME,MarketSegment.OLD)), 808);
		segmentProb.put(new HashSet<MarketSegment>(Arrays.asList(MarketSegment.FEMALE,MarketSegment.HIGH_INCOME,MarketSegment.YOUNG)), 256);
		segmentProb.put(new HashSet<MarketSegment>(Arrays.asList(MarketSegment.FEMALE,MarketSegment.HIGH_INCOME,MarketSegment.OLD)), 407);
	}

	@Override
	protected void messageReceived(Message message) {
		try {
			Transportable content = message.getContent();

			// log.fine(message.getContent().getClass().toString());

			if (content instanceof InitialCampaignMessage) {
				handleInitialCampaignMessage((InitialCampaignMessage) content);
			} else if (content instanceof CampaignOpportunityMessage) {
				handleICampaignOpportunityMessage((CampaignOpportunityMessage) content);
			} else if (content instanceof CampaignReport) {
				handleCampaignReport((CampaignReport) content);
			} else if (content instanceof AdNetworkDailyNotification) {
				handleAdNetworkDailyNotification((AdNetworkDailyNotification) content);
			} else if (content instanceof AdxPublisherReport) {
				handleAdxPublisherReport((AdxPublisherReport) content);
			} else if (content instanceof SimulationStatus) {
				handleSimulationStatus((SimulationStatus) content);
			} else if (content instanceof PublisherCatalog) {
				handlePublisherCatalog((PublisherCatalog) content);
			} else if (content instanceof AdNetworkReport) {
				handleAdNetworkReport((AdNetworkReport) content);
			} else if (content instanceof StartInfo) {
				handleStartInfo((StartInfo) content);
			} else if (content instanceof BankStatus) {
				handleBankStatus((BankStatus) content);
			} else if (content instanceof CampaignAuctionReport) {
				hadnleCampaignAuctionReport((CampaignAuctionReport) content);
			} else if (content instanceof ReservePriceInfo) {
				// ((ReservePriceInfo)content).getReservePriceType();
			} else {
				System.out.println("UNKNOWN Message Received: " + content);
			}

		} catch (NullPointerException e) {
			this.log.log(Level.SEVERE, "Exception thrown while trying to parse message." + e);
			return;
		}
	}

	private void hadnleCampaignAuctionReport(CampaignAuctionReport content) {
		// ingoring - this message is obsolete
	}

	private void handleBankStatus(BankStatus content) {
		System.out.println("Day " + day + " :" + content.toString());
	}

	/**
	 * Processes the start information.
	 *
	 * @param startInfo
	 *            the start information.
	 */
	protected void handleStartInfo(StartInfo startInfo) {
		this.startInfo = startInfo;
	}

	/**
	 * Process the reported set of publishers
	 *
	 * @param publisherCatalog
	 */
	private void handlePublisherCatalog(PublisherCatalog publisherCatalog) {
		this.publisherCatalog = publisherCatalog;
		generateAdxQuerySpace();
		getPublishersNames();

	}

	/**
	 * On day 0, a campaign (the "initial campaign") is allocated to each
	 * competing agent. The campaign starts on day 1. The address of the
	 * server's AdxAgent (to which bid bundles are sent) and DemandAgent (to
	 * which bids regarding campaign opportunities may be sent in subsequent
	 * days) are also reported in the initial campaign message
	 */
	private void handleInitialCampaignMessage(InitialCampaignMessage campaignMessage) {
		System.out.println(campaignMessage.toString());

		day = 0;

		initialCampaignMessage = campaignMessage;
		demandAgentAddress = campaignMessage.getDemandAgentAddress();
		adxAgentAddress = campaignMessage.getAdxAgentAddress();

		CampaignData campaignData = new CampaignData(initialCampaignMessage);
		campaignData.setBudget(initialCampaignMessage.getBudgetMillis() / 1000.0);
		currCampaign = campaignData;
		genCampaignQueries(currCampaign);

		/*
		 * The initial campaign is already allocated to our agent so we add it
		 * to our allocated-campaigns list.
		 */
		System.out.println("Day " + day + ": Allocated campaign - " + campaignData);
		myCampaigns.put(initialCampaignMessage.getId(), campaignData);
	}

	private ArrayList<CampaignData> getActiveCampaignsInDay(int day) {
		ArrayList<CampaignData> arr = new ArrayList<CampaignData>();
		for (CampaignData currCampaign : myCampaigns.values()) {
			if ((day >= currCampaign.dayStart) && (day <= currCampaign.dayEnd) && (currCampaign.impsTogo() > 0)) {
				arr.add(currCampaign);
			}
		}
		return arr;
	}

	/**
	 * calculate the average impression per day need to be supplied per segment
	 * 
	 * @param day
	 * @return
	 */
	private Map<MarketSegment, Double> averageNumberOfImpressionsForDay(int day) {
		Map<MarketSegment, Double> segmentAVG = new HashMap<MarketSegment, Double>();
		ArrayList<CampaignData> activeCmp = getActiveCampaignsInDay(day);
		for (CampaignData cmp : activeCmp) {
			int daysLeft = (int) (cmp.dayEnd - day) + 1;
			int impressionsLeft = cmp.impsTogo();
			double averagePerDay = impressionsLeft / daysLeft;
			// System.out.println("Campaign "+cmp.id+" has "+daysLeft+" days
			// left and "+impressionsLeft+" impressions left");
			for (MarketSegment ms : cmp.targetSegment) {
				double current = 0;
				if(segmentAVG.containsKey(ms)){
					current = segmentAVG.get(ms);
				}
				segmentAVG.put(ms, current + averagePerDay);
			}
		}
		return segmentAVG;
	}

	/**
	 * calculate how many common segments this segment has with active campaigns
	 * 
	 * @param segment
	 * @return
	 */
	private int getResembleSegmentsCampaign(Set<MarketSegment> segment) {
		ArrayList<CampaignData> activeCmp = getActiveCampaignsInDay(day);
		for(CampaignData cmp: activeCmp){
			
		}
		return 0;
	}
	
	private int getSegmentAverage(Set<MarketSegment> segment){
		switch(segment.size()){
		case 1:
			return (int)(segmentProb.getOrDefault(segment,0)/24);
		case 2:
			return (int)(segmentProb.getOrDefault(segment,0)/12);
		case 3:
			return (int)(segmentProb.getOrDefault(segment,0)/6);
		default:
			return 0;
		}
	}

	/**
	 * calculate the potential impressions for specific segment
	 * 
	 * @param segment
	 * @return
	 */
	private int getPotentialImpressions(String[] segment) {
		// TODO
		return 0;
	}

	/**
	 * calculate how much other agent may want the current campaign
	 * 
	 * @return
	 */
	private int riskFromOthers() {
		// TODO
		return 0;
	}
	
	private double potentialProfit(double bid){
		return bid*0.99;
	}

	/**
	 * remove old campaigns for other agents
	 */
	private void removeOtherOldCampaigns() {
		try {
			for (String agentName : othersCampaigns.keySet()) {
				ArrayList<CampaignData> cmparr = othersCampaigns.get(agentName);
				ArrayList<CampaignData> lst = new ArrayList<CampaignData>();
				for (CampaignData cmp : cmparr) {
					if (cmp.dayEnd >= day) {
						lst.add(cmp);
					}
				}
				othersCampaigns.put(agentName, lst);
			}
		} catch (Exception e) {
			System.out.println("~~~ERROR:" + e);
		}
	}

	/**
	 * On day n ( > 0) a campaign opportunity is announced to the competing
	 * agents. The campaign starts on day n + 2 or later and the agents may send
	 * (on day n) related bids (attempting to win the campaign). The allocation
	 * (the winner) is announced to the competing agents during day n + 1.
	 */
	private void handleICampaignOpportunityMessage(CampaignOpportunityMessage com) {

		day = com.getDay();
		// System.out.println(othersCampaigns.toString());
		//averageNumberOfImpressionsForDay(day + 2);

		pendingCampaign = new CampaignData(com);
		System.out.println("Day " + day + ": Campaign opportunity - " + pendingCampaign);

		/*
		 * The campaign requires com.getReachImps() impressions. The competing
		 * Ad Networks bid for the total campaign Budget (that is, the ad
		 * network that offers the lowest budget gets the campaign allocated).
		 * The advertiser is willing to pay the AdNetwork at most 1$ CPM,
		 * therefore the total number of impressions may be treated as a reserve
		 * (upper bound) price for the auction.
		 */
		
		Random random = new Random();
		double minBid = com.getReachImps()*0.0001/lastQualityScore;
		double maxBid = com.getReachImps()*0.001*lastQualityScore;
		double cmp_L = (1+com.getDayEnd()-com.getDayStart());
		double range = (maxBid-minBid)*Math.max(Math.pow(1.0005,winsInARow)-1,0);
		double factor = Math.min(Math.max(minBid+((maxBid-minBid)*historyFactor.getOrDefault((long)cmp_L,0.0)*0 + range),minBid),maxBid);
		long cmpimps = com.getReachImps();
		double USCAverage = USCSum/USCCount;
		long cmpBidMillis;
		if(USCAverage>=potentialProfit(factor)&&false){
			factor = maxBid;
			cmpBidMillis = (long) Math.floor(cmpimps*lastQualityScore);
		}
		else{
			cmpBidMillis = (long) Math.ceil(factor*1000);
		}
		try{
			double segmentProbability = segmentProb.getOrDefault(com.getTargetSegment(),0)/10000.0;
			/*int[] L_s = {3,5,10};
			double[] RL_s = {0.2,0.5,0.8};
			for(int l: L_s){
				for(double rl: RL_s){
					System.out.println("possible average segment: "+(cmp_L/(l*rl)));
				}
			}*/
			System.out.println("---\nSegment Users: "+segmentProbability+"\nmin bid acceptable: "+minBid+"\tmax bid acceptable: "+maxBid+"\nWins in a row: "+winsInARow+"\nbid above min: "+(factor - minBid)+"\nQuality score: "+lastQualityScore+"\nHistory factor: "+historyFactor+"\n---");
		}
		catch(Exception e){
			System.out.println("$$ ERROR: "+e);
		}
		System.out.println("Day " + day + ": Campaign total budget bid (millis): " + cmpBidMillis);
		pendingCampaign.setBid(cmpBidMillis);

		/*
		 * Adjust ucs bid s.t. target level is achieved. Note: The bid for the
		 * user classification service is piggybacked
		 */
		if (adNetworkDailyNotification != null) {
			double ucsLevel = adNetworkDailyNotification.getServiceLevel();
			ucsBid = 0.1 + random.nextDouble() / 10.0;
			USCCount++;
			USCSum += ucsBid;
			System.out.println("Day " + day + ": ucs level reported: " + ucsLevel);
		} else {
			System.out.println("Day " + day + ": Initial ucs bid is " + ucsBid);
		}
		if (getActiveCampaignsInDay(day + 1).size() == 0) {
			ucsBid = 0;
		}

		/* Note: Campaign bid is in millis */
		AdNetBidMessage bids = new AdNetBidMessage(ucsBid, pendingCampaign.id, cmpBidMillis);
		sendMessage(demandAgentAddress, bids);
	}

	/**
	 * On day n ( > 0), the result of the UserClassificationService and Campaign
	 * auctions (for which the competing agents sent bids during day n -1) are
	 * reported. The reported Campaign starts in day n+1 or later and the user
	 * classification service level is applicable starting from day n+1.
	 */
	private void handleAdNetworkDailyNotification(AdNetworkDailyNotification notificationMessage) {

		adNetworkDailyNotification = notificationMessage;

		removeOtherOldCampaigns();

		System.out.println(
				"Day " + day + ": Daily notification for campaign " + adNetworkDailyNotification.getCampaignId());

		String campaignAllocatedTo = " allocated to " + notificationMessage.getWinner();
		if ((pendingCampaign.id == adNetworkDailyNotification.getCampaignId())
				&& (notificationMessage.getCostMillis() != 0)) {
			
			if(winsInARow>=0){
				winsInARow++;
			}
			else{
				winsInARow = 1;
			}
			/* add campaign to list of won campaigns */
			pendingCampaign.setBudget(notificationMessage.getCostMillis() / 1000.0);
			currCampaign = pendingCampaign;
			genCampaignQueries(currCampaign);
			myCampaigns.put(pendingCampaign.id, pendingCampaign);
			historyFactor.put(pendingCampaign.dayEnd-pendingCampaign.dayStart+1, (notificationMessage.getCostMillis() - pendingCampaign.bid)/pendingCampaign.reachImps);
			campaignAllocatedTo = " WON at cost (Millis)" + notificationMessage.getCostMillis();
		} else {
			long days = 1 + pendingCampaign.dayEnd - pendingCampaign.dayStart;
			double curFactor = historyFactor.getOrDefault(days, 0.0);
			historyFactor.put(days, curFactor*0.05);
			winsInARow--;
			if (othersCampaigns.containsKey(notificationMessage.getWinner())) {
				ArrayList<CampaignData> lst = othersCampaigns.get(notificationMessage.getWinner());
				lst.add(pendingCampaign);
				othersCampaigns.put(notificationMessage.getWinner(), lst);
			} else {
				ArrayList<CampaignData> lst = new ArrayList<CampaignData>();
				lst.add(pendingCampaign);
				othersCampaigns.put(notificationMessage.getWinner(), lst);
			}
		}

		System.out.println("Day " + day + ": " + campaignAllocatedTo + ". UCS Level set to "
				+ notificationMessage.getServiceLevel() + " at price " + notificationMessage.getPrice()
				+ " Quality Score is: " + notificationMessage.getQualityScore());
		
		lastQualityScore = notificationMessage.getQualityScore();

	}

	/**
	 * The SimulationStatus message received on day n indicates that the
	 * calculation time is up and the agent is requested to send its bid bundle
	 * to the AdX.
	 */
	private void handleSimulationStatus(SimulationStatus simulationStatus) {
		System.out.println("Day " + day + " : Simulation Status Received");
		sendBidAndAds();
		System.out.println("Day " + day + " ended. Starting next day");
		++day;
	}

	/**
	 *
	 */
	protected void sendBidAndAds() {

		bidBundle = new AdxBidBundle();

		/*
		 * 
		 */

		int dayBiddingFor = day + 1;
		System.out.println("--------AD BIDS-------");
		System.out.println("Average impression for day " + day + " is:\n"+averageNumberOfImpressionsForDay(day));
		for (CampaignData currCampaign : getActiveCampaignsInDay(dayBiddingFor)) {
			double budgetRemains = currCampaign.budget-currCampaign.stats.getCost();
			System.out.println("Remaining budget: "+budgetRemains);
			double worstBid = currCampaign.impsTogo() / (budgetRemains* 1000);
			long daysRemains = 1 + currCampaign.dayEnd - day;
			double averagePerDay = currCampaign.impsTogo()/daysRemains;
			double percentLeft = averagePerDay/currCampaign.impsTogo();
			double rbid;
			if(day>=0 && day<6){
				rbid = worstBid * (Math.pow(1.05,percentLeft*10)-1);
			}
			else{
				rbid = worstBid * (Math.pow(1.01,percentLeft*10)-1);
			}
			
			rbid = Math.min(rbid, worstBid);
			
			System.out.println("Worst bid for impression: "+worstBid+"\nAverage impressions per day remains: "+averagePerDay+"\nPercent left: "+percentLeft+"\nBidding: "+rbid+"\n\n");
			// System.out.println("The Budget is: "+currCampaign.budget+" and
			// the required reach is: "+currCampaign.reachImps);
			// System.out.println("The worst bid for each 1000 impressions is:
			// "+worstBid);
			
			// System.out.println("The rbid is: "+rbid);

			int entCount = 0;

			for (AdxQuery query : currCampaign.campaignQueries) {
				// System.out.println((currCampaign.impsTogo() - entCount)+"
				// impressions remains for "+currCampaign.id);
				if (currCampaign.impsTogo() - entCount > 0) {
					/*
					 * among matching entries with the same campaign id, the AdX
					 * randomly chooses an entry according to the designated
					 * weight. by setting a constant weight 1, we create a
					 * uniform probability over active campaigns(irrelevant
					 * because we are bidding only on one campaign)
					 */
					if (query.getDevice() == Device.pc) {
						if (query.getAdType() == AdType.text) {
							entCount++;
						} else {
							entCount += currCampaign.videoCoef;
						}
					} else {
						if (query.getAdType() == AdType.text) {
							entCount += currCampaign.mobileCoef;
						} else {
							entCount += currCampaign.videoCoef + currCampaign.mobileCoef;
						}

					}
					bidBundle.addQuery(query, rbid, new Ad(null), currCampaign.id, 1);
				}
			}

			double impressionLimit = currCampaign.impsTogo();
			double budgetLimit = currCampaign.budget;
			bidBundle.setCampaignDailyLimit(currCampaign.id, (int) impressionLimit, budgetRemains);

			//System.out.println("Day " + day + ": Updated " + entCount + " Bid Bundle entries for Campaign id " + currCampaign.id);

		}
		System.out.println("----------------------");

		if (bidBundle != null) {
			System.out.println("Day " + day + ": Sending BidBundle");
			sendMessage(adxAgentAddress, bidBundle);
		}
	}

	/**
	 * Campaigns performance w.r.t. each allocated campaign
	 */
	private void handleCampaignReport(CampaignReport campaignReport) {

		campaignReports.add(campaignReport);

		/*
		 * for each campaign, the accumulated statistics from day 1 up to day
		 * n-1 are reported
		 */
		for (CampaignReportKey campaignKey : campaignReport.keys()) {
			int cmpId = campaignKey.getCampaignId();
			CampaignStats cstats = campaignReport.getCampaignReportEntry(campaignKey).getCampaignStats();
			CampaignData cmpg = myCampaigns.get(cmpId);
			cmpg.setStats(cstats);

			System.out.println("Day " + day + ": Updating campaign " + cmpId + " stats: " + cstats.getTargetedImps()
					+ " tgtImps " + cstats.getOtherImps() + " nonTgtImps. Cost of imps is " + cstats.getCost());
		}
	}

	/**
	 * Users and Publishers statistics: popularity and ad type orientation
	 */
	private void handleAdxPublisherReport(AdxPublisherReport adxPublisherReport) {
		if(true){
			return;
		}
		System.out.println("Publishers Report: ");
		for (PublisherCatalogEntry publisherKey : adxPublisherReport.keys()) {
			AdxPublisherReportEntry entry = adxPublisherReport.getEntry(publisherKey);
			System.out.println(entry.toString());
		}
	}

	/**
	 * @param AdNetworkReport
	 */
	private void handleAdNetworkReport(AdNetworkReport adnetReport) {

		if (true)
			return;

		System.out.println("Day " + day + " : AdNetworkReport");

		for (AdNetworkKey adnetKey : adnetReport.keys()) {

			AdNetworkReportEntry entry = adnetReport.getAdNetworkReportEntry(adnetKey);
			System.out.println(adnetKey + " " + entry);
		}

	}

	@Override
	protected void simulationSetup() {
		Random random = new Random();

		day = 0;
		bidBundle = new AdxBidBundle();

		/* initial bid between 0.2 and 0.3 */
		ucsBid = 0.2 + random.nextDouble() / 10.0;

		myCampaigns = new HashMap<Integer, CampaignData>();
		othersCampaigns = new HashMap<String, ArrayList<CampaignData>>();
		winsInARow = 0;
		lastQualityScore = 1;
		historyFactor = new HashMap<Long,Double>();
		USCSum = ucsBid;
		USCCount = 1;

		log.fine("AdNet " + getName() + " simulationSetup");
	}

	@Override
	protected void simulationFinished() {
		campaignReports.clear();
		othersCampaigns.clear();
		bidBundle = null;
	}

	/**
	 * A user visit to a publisher's web-site results in an impression
	 * opportunity (a query) that is characterized by the the publisher, the
	 * market segment the user may belongs to, the device used (mobile or
	 * desktop) and the ad type (text or video).
	 * <p/>
	 * An array of all possible queries is generated here, based on the
	 * publisher names reported at game initialization in the publishers catalog
	 * message
	 */
	private void generateAdxQuerySpace() {
		if (publisherCatalog != null && queries == null) {
			Set<AdxQuery> querySet = new HashSet<AdxQuery>();

			/*
			 * for each web site (publisher) we generate all possible variations
			 * of device type, ad type, and user market segment
			 */
			for (PublisherCatalogEntry publisherCatalogEntry : publisherCatalog) {
				String publishersName = publisherCatalogEntry.getPublisherName();
				for (MarketSegment userSegment : MarketSegment.values()) {
					Set<MarketSegment> singleMarketSegment = new HashSet<MarketSegment>();
					singleMarketSegment.add(userSegment);

					querySet.add(new AdxQuery(publishersName, singleMarketSegment, Device.mobile, AdType.text));

					querySet.add(new AdxQuery(publishersName, singleMarketSegment, Device.pc, AdType.text));

					querySet.add(new AdxQuery(publishersName, singleMarketSegment, Device.mobile, AdType.video));

					querySet.add(new AdxQuery(publishersName, singleMarketSegment, Device.pc, AdType.video));

				}

				/**
				 * An empty segments set is used to indicate the "UNKNOWN"
				 * segment such queries are matched when the UCS fails to
				 * recover the user's segments.
				 */
				querySet.add(new AdxQuery(publishersName, new HashSet<MarketSegment>(), Device.mobile, AdType.video));
				querySet.add(new AdxQuery(publishersName, new HashSet<MarketSegment>(), Device.mobile, AdType.text));
				querySet.add(new AdxQuery(publishersName, new HashSet<MarketSegment>(), Device.pc, AdType.video));
				querySet.add(new AdxQuery(publishersName, new HashSet<MarketSegment>(), Device.pc, AdType.text));
			}
			queries = new AdxQuery[querySet.size()];
			querySet.toArray(queries);
		}
	}

	/*
	 * genarates an array of the publishers names
	 */
	private void getPublishersNames() {
		if (null == publisherNames && publisherCatalog != null) {
			ArrayList<String> names = new ArrayList<String>();
			for (PublisherCatalogEntry pce : publisherCatalog) {
				names.add(pce.getPublisherName());
			}

			publisherNames = new String[names.size()];
			names.toArray(publisherNames);
		}
	}

	/*
	 * genarates the campaign queries relevant for the specific campaign, and
	 * assign them as the campaigns campaignQueries field
	 */
	private void genCampaignQueries(CampaignData campaignData) {
		Set<AdxQuery> campaignQueriesSet = new HashSet<AdxQuery>();
		for (String PublisherName : publisherNames) {
			campaignQueriesSet.add(new AdxQuery(PublisherName, campaignData.targetSegment, Device.mobile, AdType.text));
			campaignQueriesSet
					.add(new AdxQuery(PublisherName, campaignData.targetSegment, Device.mobile, AdType.video));
			campaignQueriesSet.add(new AdxQuery(PublisherName, campaignData.targetSegment, Device.pc, AdType.text));
			campaignQueriesSet.add(new AdxQuery(PublisherName, campaignData.targetSegment, Device.pc, AdType.video));
		}

		campaignData.campaignQueries = new AdxQuery[campaignQueriesSet.size()];
		campaignQueriesSet.toArray(campaignData.campaignQueries);
		//System.out.println("!!!!!!!!!!!!!!!!!!!!!!" + Arrays.toString(campaignData.campaignQueries) + "!!!!!!!!!!!!!!!!");

	}

	private class CampaignData {
		/* campaign attributes as set by server */
		Long reachImps;
		long dayStart;
		long dayEnd;
		Set<MarketSegment> targetSegment;
		double videoCoef;
		double mobileCoef;
		int id;
		private AdxQuery[] campaignQueries;// array of queries relvent for the
											// campaign.

		/* campaign info as reported */
		CampaignStats stats;
		double budget;
		double bid;

		public CampaignData(InitialCampaignMessage icm) {
			reachImps = icm.getReachImps();
			dayStart = icm.getDayStart();
			dayEnd = icm.getDayEnd();
			targetSegment = icm.getTargetSegment();
			videoCoef = icm.getVideoCoef();
			mobileCoef = icm.getMobileCoef();
			id = icm.getId();

			stats = new CampaignStats(0, 0, 0);
			budget = 0.0;
			bid = 0.0;
		}

		public void setBudget(double d) {
			budget = d;
		}
		
		public void setBid(double d){
			bid = d;
		}

		public CampaignData(CampaignOpportunityMessage com) {
			dayStart = com.getDayStart();
			dayEnd = com.getDayEnd();
			id = com.getId();
			reachImps = com.getReachImps();
			targetSegment = com.getTargetSegment();
			mobileCoef = com.getMobileCoef();
			videoCoef = com.getVideoCoef();
			stats = new CampaignStats(0, 0, 0);
			budget = 0.0;
			bid = 0.0;
		}

		@Override
		public String toString() {
			return "Campaign ID " + id + ": " + "day " + dayStart + " to " + dayEnd + " " + targetSegment + ", reach: "
					+ reachImps + " coefs: (v=" + videoCoef + ", m=" + mobileCoef + ")";
		}
		
		int impsTogo() {
			return (int) Math.max(0, reachImps - stats.getTargetedImps());
		}

		void setStats(CampaignStats s) {
			stats.setValues(s);
		}

		public AdxQuery[] getCampaignQueries() {
			return campaignQueries;
		}

		public void setCampaignQueries(AdxQuery[] campaignQueries) {
			this.campaignQueries = campaignQueries;
		}

	}

}
