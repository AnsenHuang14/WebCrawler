package com.pzoom.kis.cm5.crawler.analysis.jsoup;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.List;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import com.pzoom.kis.cm5.crawler.analysis.Analysis;
import com.pzoom.kis.cm5.crawler.category.StatusType;
import com.pzoom.kis.cm5.crawler.model.DetailPageResult;
import com.pzoom.kis.cm5.crawler.model.GPApp;
import com.pzoom.kis.cm5.crawler.model.SimilarApp;
import com.pzoom.kis.cm5.crawler.util.Log;
import com.pzoom.kis.cm5.crawler.util.PropertyUtil;
import com.pzoom.kis.cm5.crawler.util.StringUtils;

public class AppDetailJsoupAnalysis extends Analysis<DetailPageResult>{

	private static AppParser parser = new AppParser();
	private static final Log log = Log.getLogger(AppDetailJsoupAnalysis.class);
	private static final String CLIENT_TAG = PropertyUtil.getPropertyValue("clienttag").toString();
	private static final String REGION = PropertyUtil.getPropertyValue("region").toString();
	
	@Override
	public void analysis(String pkgName, String lan, long id) {
		long t = System.currentTimeMillis();
		log.info("[AppDetailJsoupAnalysis.analysis][begin]");
		resList=new ArrayList<DetailPageResult>();
		
		DetailPageResult detailPageResult = new DetailPageResult();
		GPApp gpInfo = new GPApp();
		gpInfo.setPkgName(pkgName);
		gpInfo.setLan(lan);
		gpInfo.setCountry(REGION);
		detailPageResult.setPkg(pkgName);
		detailPageResult.setLanguage(lan);
		detailPageResult.setId(id);
		detailPageResult.setClientTag(CLIENT_TAG);
		
		try{
			Document document = Jsoup.parse(htmlcontent);
			convert(document, gpInfo);
			detailPageResult.setAppInfo(gpInfo);
			detailPageResult.setSucceed(true);
			detailPageResult.setResultType(StatusType.SUCCESS);
		}catch(Exception e){
			log.error("[AppDetailJsoupAnalysis.analysis][ exception:]"+e.getMessage());
			StringWriter sw = new StringWriter();
			PrintWriter pw = new PrintWriter(sw);
			e.printStackTrace(pw);
			log.error(sw.toString());
			detailPageResult.setAppInfo(gpInfo);
			detailPageResult.setErrorMsg("analysis error:"+e.getMessage());
			detailPageResult.setSucceed(false);
			detailPageResult.setResultType(StatusType.ANALYSIS_EXCEPTION);
		}
		
		resList.add(detailPageResult);
		log.info("[AppDetailJsoupAnalysis.analysis][cost:{0}ms]",System.currentTimeMillis()-t);
		log.info("[AppDetailJsoupAnalysis.analysis][end--]");
	}

	private void convert(Document document,GPApp gpApp){
		List<SimilarApp> simiarityApps = parser.parseSimilarityApp(document);
		gpApp.setSimilarApps(simiarityApps);
		
		String gpTag = parser.parseGpTag(document);
		gpApp.setGpTag(gpTag);
		
		String developer = parser.parseDeveloper(document);
		gpApp.setDeveloper(developer);
		gpApp.setName(developer);
		
		String developerEncodedStr = parser.parseEncodedDeveloperStr(document);
		gpApp.setDeveloperEncodedStr(developerEncodedStr);
		
		String title = parser.parseTitle(document);
		if(title==null){
			log.error("title null");
			log.error(document);
		}
		gpApp.setTitle(title);
		
		String appType = parser.parseAppType(document);
		gpApp.setAppType(appType);
		
		String gpCategory = parser.parseGpCategory(document);
		gpApp.setGpCategory(gpCategory);
		String gpCategory2 = parser.parseGpCategory2(document);
		gpApp.setGpCategory2(gpCategory2);
		String clkUrl = parser.parseClkUrl(document);
		gpApp.setClkUrl(clkUrl);
		
//		String country = parser.parseCountry(document);
//		gpApp.setCountry(country);
//		
//		String lan = parser.parseLan(document);
//		gpApp.setLan(lan);
		
		String logoUrl = parser.parseLogoUrl(document);
		gpApp.setLogoUrl(logoUrl);
		
		String price = parser.parsePrice(document);
		gpApp.setPrice(price);
		
		String rating  = parser.parseRating(document);
		gpApp.setRating(rating);
		
		String reviewers = parser.parseReviewers(document);
		gpApp.setReviewers(reviewers);
		
		String contentRating = parser.parseContentRating(document);
		gpApp.setContentRating(contentRating);
		
		String datePublished = parser.parseDatePublished(document);
		gpApp.setDatePublished(datePublished);
		
		String fileSize = parser.parseFileSize(document);
		gpApp.setFileSize(fileSize);
		
		String genre = parser.parseGenre(document);
		gpApp.setGenre(genre);
		String genre2 = parser.parseGenre2(document);
		gpApp.setGenre2(genre2);
		
		
		
		String numDownloads = parser.parseNumDownloads(document);
		gpApp.setNumDownloads(numDownloads);
		
		String operatingSystem = parser.parseOperatingSystem(document);
		gpApp.setOperatingSystems(operatingSystem);
		
		String ratingValue = parser.parseRatingValue(document);
		gpApp.setRatingValue(ratingValue);
		
		String softwareVersion = parser.parseSoftwareVersion(document); 
		gpApp.setSoftwareVersion(softwareVersion);
		
		String desc = parser.parseDesc(document);
		gpApp.setDesc(desc);
		
		String whatsNew = parser.parseWhatsNew(document);
		gpApp.setWhatsNew(whatsNew);
		
		String thumbnails = parser.parseThumbnails(document);
		gpApp.setThumbnails(thumbnails);
		
		String ratingHistogram = parser.parseRatingHistogram(document);
		gpApp.setRatingHistogram(ratingHistogram);
		
		//add app buys
		String inappMsg  = parser.parseInappMsg(document);
		gpApp.setInAppMsg(inappMsg);
		String mail  = parser.parseMail(document);
		gpApp.setMail(mail);
	}
	
	
	private static class AppParser implements IParseAppDetail{
		private static final Log logger = Log.getLogger(AppParser.class);
		@Override
		public List<SimilarApp> parseSimilarityApp(Document document) {
			logger.info("similar apps begin");
			List<SimilarApp> simiarityApps = new ArrayList<SimilarApp>();
			Elements clusElements  = document.select("div.rec-cluster");
			if(clusElements!=null&&clusElements.size()>0){
				Element childElemens = clusElements.get(0);
				Elements elements = childElemens.select("div.details");
				if(elements!=null){
					for(Element e:elements){
						SimilarApp  similarApp = new SimilarApp(); 
						Elements detailUrls = e.select("a.card-click-target");
						if(detailUrls.size()>0){
							Element url = detailUrls.get(0);
							String appUrl = url.attr("href");
							int index = -1;
							if((index=appUrl.indexOf("="))!=-1){
								String app = appUrl.substring(index+1);
									similarApp.setPkg(app);
							}		
						}
						Elements developers = e.select("div.subtitle-container a.subtitle");
						if(developers.size()>0){
							Element deveElement = developers.get(0);
							String devUrl = deveElement.attr("href");
							int index = -1;
							if((index=devUrl.indexOf("id="))!=-1){
								String developer = devUrl.substring(index+"id=".length());
								similarApp.setDeveloper(developer);
							}		
						}
						simiarityApps.add(similarApp);
					}
				}
			}
			
			logger.info("similarApp size:{0}",simiarityApps.size());
			return simiarityApps;
		}

		@Override
		public String parseGpTag(Document document) {
			Elements elements = document.select("span.badge-title");
			String topDev = null;
			Element e = firstElement(elements);
			if(e!=null){
				String txt = e.text();
				if(StringUtils.hashLength(txt)){
					topDev = txt;
				}
			}
			return topDev;
		}

		@Override
		public String parseDeveloper(Document document) {
			String developer = null;
			Elements elements = document.select("a.document-subtitle").select("a.primary").select("span[itemprop=name]");
		    Element e = firstElement(elements);
		    if(e!=null){
		    	developer = e.text();
		    }
			return developer;
		}

		@Override
		public String parseTitle(Document document) {
			String title = null;
			Elements elements = document.select(".document-title[itemprop=name] div");
			Element e = firstElement(elements);
			if(e!=null){
				title = e.text();
			}
			return title;
		}
		
		
		
		private Element firstElement(Elements elements){
			Element e = null;
			if(elements!=null&&elements.size()>0){
				e = elements.first();
			}
			return e;
		}

		@Override
		public String parseAppType(Document document) {
			String type = null;
			Elements elements = document.select("a.document-subtitle").select("a.category");
			Element e = firstElement(elements);
			if(e!=null){
				String href =e.attr("href");
				int index = -1;
				if(href!=null&&(index=href.lastIndexOf("/"))!=-1){
					type = href.substring(index+1)+"/collection/topselling_free";
				}
			}
			return type;
		}

		@Override
		public String parseGpCategory(Document document) {
			String gpCategory = null;
			Elements elements = document.select("a.document-subtitle").select("a.category");
			Element e = firstElement(elements);   
			

			if(e!=null){
				String href = e.attr("href");
				int index = -1;
				if(href!=null&&(index=href.lastIndexOf("/"))!=-1){
					gpCategory = href.substring(index+1);
				}
			}
			return gpCategory;
		}
		@Override
		public String parseGpCategory2(Document document) {
			String gpCategory = null;
			Elements elements = document.select("a.document-subtitle").select("a.category");
			Element e = null;
			if (elements != null && elements.size() > 1) {
               e = elements.get(1);
              }

			if (e != null) {
				String href = e.attr("href");
				int index = -1;
				if (href != null && (index = href.lastIndexOf("/")) != -1) {
					gpCategory = href.substring(index + 1);
				}
			}
			return gpCategory;
		}

		@Override
		public String parseClkUrl(Document document) {
			String clkUrl = null;
			Elements elements = document.select("div#body-content[itemtype=http://schema.org/MobileApplication][itemscope=itemscope][role=main] meta[itemprop=url]");
			Element e = firstElement(elements);
			if(e!=null){
				clkUrl = e.attr("content");
			}
			return clkUrl;
		}

//		@Override
//		public String parseCountry(Document document) {
//			String country = null;
//			Elements elements = document.select("link[rel=canonical]");
//			Element e = firstElement(elements);
//			if(e!=null){
//				String href = e.attr("href");
//				int index = -1;
//				if(href!=null&&(index=href.lastIndexOf("hl"))!=-1){
//					String countryStr = href.substring(index+"hl".length()+1);
//					String[] array =countryStr.split("_");
//					if(!ObjectUtils.isEmpty(array)){
//						country = array[0];
//					}
//				}
//			}
//			return country;
//		}

//		@Override
//		public String parseLan(Document document) {
//			String lan = null;
//			Elements elements = document.select("link[rel=canonical]");
//			Element e = firstElement(elements);
//			if(e!=null){
//				String href = e.attr("href");
//				int index = -1;
//				if(href!=null&&(index=href.lastIndexOf("hl"))!=-1){
//					String countryStr = href.substring(index+"hl".length()+1);
//					String[] array =countryStr.split("_");
//					if(!ObjectUtils.isEmpty(array)){
//						lan = array[0];
//						if(array.length>1){
//							lan = array[1];
//						}
//					}
//				}
//			}
//			return lan;
//		}

		@Override
		public String parseLogoUrl(Document document) {
			String logoUrl = null;
			Elements elements = document.select("div.cover-container img.cover-image");
			Element e = firstElement(elements);
			if(e!=null){
				logoUrl = e.attr("src");
			}
			return logoUrl;
		}

		@Override
		public String parsePrice(Document document) {
			String price = null;
			Elements elements = document.select("meta[itemprop=offerType]+meta[itemprop=price]");
			Element e = firstElement(elements);
			if(e!=null){
				String value = e.attr("content");
				if(StringUtils.hashLength(value)&&value.equals("0")){
					price = "Install";
				}else{
					price = value;
				}
			}
			return price;
		}

		@Override
		public String parseRating(Document document) {
			String rating = null;
			Elements elements = document.select("div.current-rating");
			Element e = firstElement(elements);
			if(e!=null){
				String widthStr = e.attr("style");
			    if(widthStr!=null){
			    	rating = widthStr.replace("width", "").replace(":", "").replace(";", "").replace(" ", "");
			    }
			}
			return rating;
		}

		@Override
		public String parseReviewers(Document document) {
			String reviewers = null;
			Elements elements = document.select("span.reviews-num");
			Element e = firstElement(elements);
			if(e!=null){
				reviewers = e.text();
			}
			return reviewers;
		}

		@Override
		public String parseContentRating(Document document) {
			String contentRaging = null;
			Elements elements = document.select("div.content[itemprop=contentRating]");
			Element e = firstElement(elements);
			if(e!=null){
				contentRaging = e.text();
			}
			return contentRaging;
		}

		@Override
		public String parseRatingValue(Document document) {
			String ratingValue = null;
			Elements elements = document.select("div.score-container[itemscope=itemscope][itemprop=aggregateRating][itemtype=http://schema.org/AggregateRating] meta[itemprop=ratingValue]");
			Element e = firstElement(elements);
			if(e!=null){
				ratingValue = e.attr("content");
			}
			return ratingValue;
		}

		@Override
		public String parseSoftwareVersion(Document document) {
			String softwareVersion = null;
			Elements elements = document.select("div.content[itemprop=softwareVersion]");
			Element e = firstElement(elements);
			if(e!=null){
				softwareVersion = e.text();
			}
			return softwareVersion;
		}

		@Override
		public String parseDesc(Document document) {
			String desc = null;
			//Elements elements = document.select("div.id-app-orig-desc");
			Elements elements = document.select("div.show-more-content.text-body").select("div[jsname=C4s9Ed]");
			Element e = firstElement(elements);
			if(e!=null){
				desc = e.html();
			}
			return desc;
		}

		@Override
		public String parseWhatsNew(Document document) {
			String whatsNew = null;
			Elements elements = document.select("div.recent-change");
			if(elements!=null){
				StringBuilder sb = new StringBuilder();
				for(Element e:elements){
					String txt = e.text();
					if(txt!=null){
						sb.append(txt).append("\n");
					}
				}
				whatsNew = sb.toString();
			}
			return whatsNew;
		}

		@Override
		public String parseDatePublished(Document document) {
			String datePublished = null;
			Elements elements = document.select("div.content[itemprop=datePublished]");
			Element e = firstElement(elements);
			if(e!=null){
				datePublished = e.text();
			}
			return datePublished;
		}

		@Override
		public String parseFileSize(Document document) {
			String fileSize = null;
			Elements elements = document.select("div.content[itemprop=fileSize]");
			Element e = firstElement(elements);
			if(e!=null){
				fileSize = e.text();
			}
			return fileSize;
		}

		@Override
		public String parseGenre(Document document) {
			String genre = null;
			Elements elements = document.select("span[itemprop=genre]");
			Element e = firstElement(elements);

			if (e != null) {
				genre = e.text();
			}
			return genre;
		}
		@Override
		public String parseGenre2(Document document) {
			String genre = null;
			Elements elements = document.select("span[itemprop=genre]");
			Element e = null;
			if (elements != null && elements.size() > 1) {
				e = elements.get(1);
			}

			if (e != null) {
				genre = e.text();
			}
			return genre;
		}

		@Override
		public String parseNumDownloads(Document document) {
			String numDownloads = null;
			Elements elements = document.select("div.content[itemprop=numDownloads]");
			Element e = firstElement(elements);
			if(e!=null){
				numDownloads = e.text();
			}
			return numDownloads;
		}

		@Override
		public String parseOperatingSystem(Document document) {
			String operatingSystem = null;
			Elements elements = document.select("div.content[itemprop=operatingSystems]");
			Element e = firstElement(elements);
			if(e!=null){
				operatingSystem = e.text();
			}
			return operatingSystem;
		}

		@Override
		public String parseThumbnails(Document document) {
			String thumbnails = null;
			List<String> urlList = new ArrayList<String>();
			Elements elements = null;
			elements = document.select("img.screenshot").select("img.clickable");
			if(elements!=null&&elements.size()>0){
				for(Element e :elements){
					String src = e.attr("src");
					if(StringUtils.hashLength(src)){
						if(!urlList.contains(src)){
							urlList.add(src);
						}
					}
				}
			}
			
			elements = document.select("img.full-screenshot").select("img.clickable");
			if(elements!=null&&elements.size()>0){
				for(Element e :elements){
					String src = e.attr("src");
					if(StringUtils.hashLength(src)){
						if(!urlList.contains(src)){
							urlList.add(src);
						}
					}
				}
			}
			
			elements = document.select("img.screenshot");
			if(elements!=null&&elements.size()>0){
				for(Element e :elements){
					String src = e.attr("src");
					if(StringUtils.hashLength(src)){
						if(!urlList.contains(src)){
							urlList.add(src);
						}
					}
				}
			}
			if(urlList.size()>0){
				StringBuilder sb =new StringBuilder();
				for(String url:urlList){
					sb.append(url).append("\n");
				}
				thumbnails = sb.toString();
			}
			return thumbnails;
		}

		@Override
		public String parseRatingHistogram(Document document) {
			String ratingHistogram = null;
			StringBuilder sb = new StringBuilder();
			Elements elements = null;
			Element e = null;
			//五星评价
			elements = document.select("div.rating-bar-container").select("div.five").select("span.bar-number");
			e = firstElement(elements);
			if(e!=null){
				String n = e.text();
				if(n!=null){
					n = n.replaceAll(",", "");
					sb.append(n).append(",");
				}
			}
			
			//四星评价
			elements = document.select("div.rating-bar-container").select("div.four").select("span.bar-number");
			e = firstElement(elements);
			if(e!=null){
				String n = e.text();
				if(n!=null){
					n = n.replaceAll(",", "");
					sb.append(n).append(",");
				}
			}
			
			
			//三星评价
			elements = document.select("div.rating-bar-container").select("div.three").select("span.bar-number");
			e = firstElement(elements);
			if(e!=null){
				String n = e.text();
				if(n!=null){
					n = n.replaceAll(",", "");
					sb.append(n).append(",");
				}
			}
			
			
			//二星评价
			elements = document.select("div.rating-bar-container").select("div.two").select("span.bar-number");
			e = firstElement(elements);
			if(e!=null){
				String n = e.text();
				if(n!=null){
					n = n.replaceAll(",", "");
					sb.append(n).append(",");
				}
			}
			
			//一星评价
			elements = document.select("div.rating-bar-container").select("div.one").select("span.bar-number");
			e = firstElement(elements);
			if(e!=null){
				String n = e.text();
				if(n!=null){
					n = n.replaceAll(",", "");
					sb.append(n);
				}
			}
			
			if(sb.length()>0){
				ratingHistogram = sb.toString();
			}
			return ratingHistogram;
		}

		@Override
		public String parseInappMsg(Document document) {
			Elements elements = document.select("div.info-container").select("div.inapp-msg");
			Element e = firstElement(elements);
			if(e!=null){
				return e.text();
			}
			return null;
		}

		@Override
		public String parseMail(Document document) {
			Elements elements = document.select("div.content.contains-text-link").select("a.dev-link");
			if (elements != null && elements.size() > 0) {
				for (Element e : elements) {
					String href = e.attr("href");
					if (href != null) {
						if (href.contains("mailto:")) {
							return href.substring(href.indexOf("mailto:") + 7);
						}
					}
				}

			}

			return null;
		}

		@Override
		public String parseEncodedDeveloperStr(Document document) {
			Elements elements = document.select("div.details-info div.info-container div[itemprop=author]").select("a.document-subtitle").select("a.primary");
			Element e = firstElement(elements);
			if(e!=null){
				String href = e.attr("href");
				if(href!=null){
					int index = href.indexOf("id=");
					if(index!=-1){
					    String devStr = href.substring(index+"id=".length());
					    return devStr;
					}
				}
			}
			return null;
		}
	}

}
