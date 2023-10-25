# Databricks notebook source
# MAGIC %md
# MAGIC ###Processes the data from stb_viewership.viewership_fact table and loads into stb_viewership.linear_viewership_dtv table.###         
# MAGIC **The notebook handles the below scenarios based on the input being provided manually or auto**
# MAGIC
# MAGIC Scenario-1: Auto (Regular Run - Monthly)
# MAGIC No input given - The job will execute current-1 months data
# MAGIC Checks for the derived table if we have current-1 month's data.If the data is there do nothing succeed with a message data already exists for the given month.If data is not there continue processing
# MAGIC
# MAGIC Scenario-2: Manual input on demand
# MAGIC Input month is given - Execute the query providing year and month manually in cmd4 and comment out cmd8. Based on the input month,year overwrites the data in derived table if data for that month is already present. Input is month and year
# MAGIC Delete and Insert

# COMMAND ----------

dbutils.widgets.text('notebook_config', '')
dbutils.widgets.text('execution_id', '')

# COMMAND ----------

import pandas as pd
import numpy as np
from numpy.random import randint
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import date, timedelta, datetime
import json

# COMMAND ----------

#year = f"year(current_date())"
#month = f"month(add_months(current_date(), -4))"
tgtTableName = "stb_viewership.DTV_Test"
#year = '2023'
#month = '6'

# COMMAND ----------

dtv_viewership_qry = f'''select year(start_date) as Year, month(start_date) as Month,
case 
when lower(source_filename) LIKE '%atl%' THEN 'ATL'
when lower(source_filename)  LIKE '%on%' THEN 'ON'
when lower(source_filename)  LIKE '%sc%' THEN 'SC'
end as region,
count(distinct CAN) as viewers, 
count(distinct device_id) as tuners,
sum(duration_min) as mins,
CASE 
when channel_desc in('1FREE') then '1FREE'
when channel_desc in (CONCAT('A','&','E') ,CONCAT('HDA','&','E')) then 'AE'
when channel_desc in('AAJTK') then 'Aaj Tak'
when channel_desc in('AATH') then 'SONY Aath'
when channel_desc in('ASide','Aside') then 'A.Side'
when channel_desc in('ABPN') then 'ABP News'
when channel_desc in('HDABC','WKBW') then 'ABC Buffalo (WKBW)'
when channel_desc in('HABCW','KOMO') then 'ABC Seattle (KOMO)'
when channel_desc in('ABCSH','ABCSP') then 'Spark ROD'
when channel_desc in('AFOOD') then 'ATN Food Food'
when channel_desc in('AFRO') then 'AFROGLOBAL TELEVISION'
when channel_desc in('AHC') then 'American Heroes'
when channel_desc in('AJE') then 'Al Jazeera English'
when channel_desc in('ALERT') then 'Emergency Alerting Service'
when channel_desc in('ALIFE') then 'ALIFE'
when channel_desc in('ALLK') then 'ALL TV-K'
when channel_desc in('ALLTV') then 'All TV (Korean)'
when channel_desc in('ALNAH') then 'Al Nahar Drama '
when channel_desc in('ALPHA') then 'Alpha Sat'
when channel_desc in('ACTHD','ACTN') then 'Action'
when channel_desc in('AMIau') then 'AMI-audio'
when channel_desc in('AMITL') then 'AMI-télé'
when channel_desc in('AMItv') then 'AMItv'
when channel_desc in('addik','adkHD') then 'addik TV'
when channel_desc in('AOVMC') then 'AOV Adult Movie Channel'
when channel_desc in('AOVMF') then 'AOV Maleflixxx TV'
when channel_desc in('AOVXX') then 'AOV XXX Action Clips TV'
when channel_desc in('ADSM','ADSMH') then 'Adult Swim'
when channel_desc in('AMC','AMCHD') then 'AMC'
when channel_desc in('ARAB') then 'Abu Dhabi TV'
when channel_desc in('ART') then 'ART Cable (Arabic)'
when channel_desc in('ANML','ANMLH') then 'Animal Planet'
when channel_desc in('ARYD') then 'ARY Digital USA'
when channel_desc in('ARYN') then 'ARY News'
when channel_desc in('APTN','APTNH') then 'APTN'
when channel_desc in('ASTHA') then 'AASTHA Television Channel'
when channel_desc in('AQAHD','AQUAR') then 'Aquarium Channel'
when channel_desc in('ATNB') then 'ATN Bangla'
when channel_desc in('ATNCB') then 'ATN Colors Bangla'
when channel_desc in('ATNCM') then 'ATN Colors Marathi'
when channel_desc in('ATNGJ') then 'ATN Gujarati'
when channel_desc in('ATNMO') then 'ATNMO'
when channel_desc in('ATNMP') then 'ATNMP'
when channel_desc in('ATNNW') then 'ATN News 18'
when channel_desc in('ATNP5') then 'ATN Punjabi 5'
when channel_desc in('ATNPM') then 'ATNPM'
when channel_desc in('ATNPN') then 'ATN Punjabi News'
when channel_desc in('ATNS') then 'ATN Sports'
when channel_desc in('ATNTP') then 'ATNTP'
when channel_desc in('AZMUN') then 'Az Mundo (formerly Azteca 13 Internacional)'
when channel_desc in('ATN','ATNHD') then 'ATN'
when channel_desc in('B4U') then 'B4U Movies'
when channel_desc in('B4UMU') then 'B4U Music'
when channel_desc in('BABY') then 'Baby TV'
when channel_desc in('BABY1') then 'Baby First TV'
when channel_desc in('BARRI') then 'BARRI'
when channel_desc in('BASIA') then 'ATN Brit Asia'
when channel_desc in('BBCAR') then 'BBC Arabic'
when channel_desc in('BBCC') then 'BBC Canada'
when channel_desc in('BBCE') then 'BBC Earth'
when channel_desc in('BBCK') then 'BBC Kids'
when channel_desc in('BBCW') then 'BBC World News'
when channel_desc in('BEIN') then 'beIN SPORTS'
when channel_desc in('beINE') then 'beIN Sports en Español'
when channel_desc in('BEJTV') then 'Beijing TV (BTV)'
when channel_desc in('BET') then 'BET'
when channel_desc in('BIGM') then 'Big Magic International'
when channel_desc in('BIMBI') then 'TeleBimbi'
when channel_desc in('BNN') then 'BNN (Business News Network)'
when channel_desc in('BNNB') then 'BNNB'
when channel_desc in('BNNBL') then 'BNNBL'
when channel_desc in('BOOK') then 'BookTelevision'
when channel_desc in('B10','B10HD') then 'Big Ten Network'
when channel_desc in('BTV') then 'BTV (Benefica TV)'
when channel_desc in('CANLN') then 'Le Canal Nouvelles TVA (LCN)'
when channel_desc in('CARDO') then 'Caribbean Radio'
when channel_desc in('CARIB') then 'ATN CBN (Commonwealth Broadcasting Network)'
when channel_desc in('CASA') then 'CASA'
when channel_desc in('CBCL') then 'CBCL FM London'
when channel_desc in('BRAVO','BRVOH') then 'Bravo'
when channel_desc in('CBHT') then 'CBC Halifa (CBHT)'
when channel_desc in('CBL') then 'CBC Radio Two (CBL)'
when channel_desc in('CBLA') then 'CBC Radio One Toronto CBLA FM'
when channel_desc in('CBLFT') then 'ICI Radio-Canada Télé (TOR)'
when channel_desc in('CBLT') then 'CBC Toronto (CBLT)'
when channel_desc in('CBMT') then 'CBC Montreal (CBMT)'
when channel_desc in('CBNT') then 'CBC St. Johns (CBNT)'
when channel_desc in('CBO','HDCBO') then 'CBC Radio One Ottawa  (English)'
when channel_desc in('CBOF') then 'CBC Radio One Ottawa (French)'
when channel_desc in('CBOFT') then 'ICI Radio-Canada Télé (OTT)'
when channel_desc in('CBOT') then 'CBC Ottawa (CBOT)'
when channel_desc in('CBRT') then 'CBC Calgary (CBRT)'
when channel_desc in('CBWT') then 'CBC Winnipeg (CBWT)'
when channel_desc in('CCENT') then 'CCTV Ent'
when channel_desc in('CCTV4') then 'CCTV-4'
when channel_desc in('CEEN') then 'CEEN'
when channel_desc in('CFCA') then 'CFCA FM Waterloo'
when channel_desc in('CFGO') then 'CFGO AM 1200 - Ottawa - Sports'
when channel_desc in('CFGS') then 'CFGS'
when channel_desc in('CFHK') then 'CFHK FM London'
when channel_desc in('CFJP') then 'V Montreal (CFJP)'
when channel_desc in('CFMZ') then 'Classical 96.3FM Toronto (CFMZ)'
when channel_desc in('CFNY') then '102.1 The Edge Toronto (CFNY FM)'
when channel_desc in('CFRA') then '580 CFRA Ottawa (CFRA AM)'
when channel_desc in('CFRB') then 'CFRB AM News/Talk 1010 Toronto '
when channel_desc in('CFRH') then 'Radio Huronie - Penetanguishene'
when channel_desc in('CFRU') then 'CFRU FM Guelph'
when channel_desc in('CFTM') then 'TVA Montreal (CFTM)'
when channel_desc in('CFTR') then '680 NEWS Toronto (CFTR AM)'
when channel_desc in('CFXJ') then '93.5 The Move'
when channel_desc in('CFZM') then 'CFZM-AM 740 '
when channel_desc in('CGTN') then 'CGTN-News'
when channel_desc in('CH M') then 'CH M'
when channel_desc in('CH1RU') then 'Channel One Russia'
when channel_desc in('CHBM') then 'boom 97.3'
when channel_desc in('CNLD','DHD') then 'Canal D'
when channel_desc in('CHCR') then 'CHCR Greek Radio'
when channel_desc in('CHEX') then 'CHEX'
when channel_desc in('CHEX2') then 'CHEX2'
when channel_desc in('CHEZ') then 'CHEZ 106 FM Ottawa'
when channel_desc in('CHFI') then '98.1 CHFI Toronto'
when channel_desc in('CHFN') then 'CHFN-FM Aboriginal Radio Wiarton'
when channel_desc in('CHHA') then 'CHHA AM 1610 Spanish Radio Toronto'
when channel_desc in('CHIR') then 'CHIR FM Greek Radio Toronto'
when channel_desc in('CHLX') then 'Planète 97.1 - French (CHLX FM)'
when channel_desc in('CHOQ') then 'CHOQ-FM 105.1 Toronto'
when channel_desc in('EVAHD','EVAS') then 'Canal Évasion'
when channel_desc in('CHRW') then 'CHRW FM London'
when channel_desc in('CHRY') then 'CHRY 105.5 FM York U Toronto'
when channel_desc in('CHST') then '102.3 JACK FM'
when channel_desc in('CHUM') then '104.5 CHUM FM Toronto'
when channel_desc in('CHUO') then 'CHUO FM 89.1 U.of Ottawa'
when channel_desc in('CHYM') then 'CHYM FM Kitchener'
when channel_desc in('CI') then 'Crime + Investigation'
when channel_desc in('CIDC') then 'Z103.5 Toronto (CIDC)'
when channel_desc in('CIHT') then 'HOT 89.9 FM Ottawa (CIHT FM)'
when channel_desc in('CIKZ') then 'Country 106.7 FM'
when channel_desc in('CIMJ') then 'CIMJ FM Guelph'
when channel_desc in('CINE') then 'Cinelatino'
when channel_desc in('CING') then 'CING'
when channel_desc in('CIQM') then 'CIQM FM London'
when channel_desc in('CISS') then '105.3 KISS FM Ottawa (CISS FM)'
when channel_desc in('CITYC') then 'City Calgary'
when channel_desc in('CITYM','CTYM.') then 'City Montreal'
when channel_desc in('VIE','VIEHD') then 'Canal Vie'
when channel_desc in('CITYW') then 'City Winnipeg'
when channel_desc in('CIUT') then 'CIUT FM 89.5 U. of T. Toronto'
when channel_desc in('CBCNN','NN HD') then 'CBC News Network'
when channel_desc in('CIWW') then '1310News Ottawa '
when channel_desc in('CIXX') then 'CIXX FM London'
when channel_desc in('CJAX') then '96.9 JACK FM  Vancouver (CJAX)'
when channel_desc in('CJBC') then 'CJBC AM 860 Toronto Premiere Chaine'
when channel_desc in('CJBX') then 'CJBX FM London'
when channel_desc in('CJCH') then 'CJCH'
when channel_desc in('CJCL') then 'THE FAN 590 Toronto (CJCL)'
when channel_desc in('CJET') then '92.3 JACK FM Smiths Falls (CJET FM)'
when channel_desc in('CJIQ') then 'CJIQ FM Kitchener'
when channel_desc in('CJMJ') then 'Majic 100 Ottawa (CJMJ FM)'
when channel_desc in('CJRT') then 'JAZZ.FM91 Toronto (CJRT)'
when channel_desc in('CJTW') then 'CJTW FM Kitchener'
when channel_desc in('CKBT') then 'CKBT FM Kitchener'
when channel_desc in('CKBY') then 'Country 101.1 Smiths Falls'
when channel_desc in('CKCU') then 'CKCU 93.1FM Carleton U. Ottawa'
when channel_desc in('CKDJ') then 'CKDJ 107.9FM Algonquin College Ottawa'
when channel_desc in('CKDK') then 'More 103.9 FM'
when channel_desc in('CKFM') then '999 VIRGIN RADIO'
when channel_desc in('CKGL') then 'CKGL AM Kitchener'
when channel_desc in('CKMS') then 'CKMS FM Waterloo'
when channel_desc in('CKOT') then 'CKOT FM Tillsonburg'
when channel_desc in('CKPLS') then 'ATN Cricket Plus'
when channel_desc in('CKRZ') then 'CKRZ FM Ohsweken'
when channel_desc in('CKTF') then '104.1FM Ottawa - French (CKTF FM)'
when channel_desc in('CKUN') then 'CKUN-FM Aboriginal Radio Christian Isle'
when channel_desc in('CKWR') then 'CKWR FM Waterloo'
when channel_desc in('CBUT','HCBCV') then 'CBC Vancouver (CBUT)'
when channel_desc in('CMDYG') then 'Comedy Gold'
when channel_desc in('CMT') then 'CMT Canada (Country Music Television)'
when channel_desc in('CN') then 'Cartoon Network'
when channel_desc in('CNBC') then 'CNBC (Consumer News and Business Channel)'
when channel_desc in('HDCBS','WIVB') then 'CBS Buffalo (WIVB)'
when channel_desc in('HCBSW','KIRO') then 'CBS Seattle (KIRO)'
when channel_desc in('CNNI') then 'CNN International'
when channel_desc in('COLRS') then 'Aapka Colors'
when channel_desc in('COMDY') then 'COMDY'
when channel_desc in('CHCH','HD CH') then 'CHCH'
when channel_desc in('CHRGD','CHRHD') then 'CHRGD'
when channel_desc in('CITYT','HDCTY') then 'City Toronto'
when channel_desc in('CITYV','HCTYV') then 'City Vancouver'
when channel_desc in('CPACE') then 'CPAC English'
when channel_desc in('CPACF') then 'CPAC French'
when channel_desc in('CPACT') then 'CPACT'
when channel_desc in('CPUNJ') then 'Channel Punjabi'
when channel_desc in('CPWA') then 'CPWA FM Portuguese Radio Toronto'
when channel_desc in('CRCTV') then 'Caracol TV Internacional'
when channel_desc in('CNN','CNNHD') then 'CNN'
when channel_desc in('COOK','COOKH') then 'Cooking Channel'
when channel_desc in('CTV2E') then 'CTV Two Atlantic'
when channel_desc in('COSMH','COSMO') then 'Cosmopolitan TV'
when channel_desc in('CTV2V') then 'CTV Two Vancouver Island'
when channel_desc in('CTVBC') then 'CTV Vancouver (CTVBC)'
when channel_desc in('CTVCA') then 'CTV Calgary (CTVCA)'
when channel_desc in('CTVML') then 'CTV Montreal (CTVML)'
when channel_desc in('COTT','COTTH') then 'Cottage Life'
when channel_desc in('CP24','CP24H') then 'CP24'
when channel_desc in('CTVSH') then 'CTVSH'
when channel_desc in('CTVSO') then 'CTV Kitchener/London (CTVSO)'
when channel_desc in('CTVWN') then 'CTV Winnipeg (CTVWN)'
when channel_desc in('CYRTV') then 'China Yellow River Television'
when channel_desc in('DAYS') then 'Daystar Television Network Canada'
when channel_desc in('DDBRT') then 'ATN DD Bharati'
when channel_desc in('DDIND') then 'ATN DD India'
when channel_desc in('DDNWS') then 'ATN DD News'
when channel_desc in('DEJA') then 'DejaView'
when channel_desc in('CRV1','CRV2','CRV3','CRV4') then 'Crave'
when channel_desc in('DISFR') then 'La Chaine Disney'
when channel_desc in('DISJR','DJR') then 'DisneyJnr'
when channel_desc in('DISNY','DISN') then 'Disney Channel'
when channel_desc in('DISXD','DISX') then 'DisneyXDC'
when channel_desc in('DIY') then 'Do It Yourself'
when channel_desc in('CTVNC','CTVNH') then 'CTV News Channel'
when channel_desc in('DORCE') then 'DORCE'
when channel_desc in('DRAGN') then 'Dragon TV'
when channel_desc in('DRAMA') then 'DRAMA'
when channel_desc in('DRM2') then 'Dream 2'
when channel_desc in('DRMHD') then 'DRMHD'
when channel_desc in('CTVOT','HCTVO') then 'CTV Ottawa (CTVOT)'
when channel_desc in('DURTV') then 'DURTV'
when channel_desc in('DVELO') then 'Discovery Velocity'
when channel_desc in('DW') then 'DW'
when channel_desc in('DW D+') then 'DW (Deutsch+)'
when channel_desc in('CTVTO','HDCTV') then 'CTV Toronto (CTVTO)'
when channel_desc in('ELLE') then 'ELLE'
when channel_desc in('CTV2L','LCTV2') then 'CTV Two London'
when channel_desc in('ERTW') then 'ERT World'
when channel_desc in('ESPNC') then 'ESPN Classic Canada'
when channel_desc in('ESTRA') then 'Canal de las Estrellas'
when channel_desc in('EURNW') then 'EuroNews'
when channel_desc in('CTV2O','OCTV2') then 'CTV Two Ottawa'
when channel_desc in('EWS') then 'EuroWorldSports'
when channel_desc in('EWTN') then 'Eternal Word Television Network'
when channel_desc in('EXTSY') then 'Exxxtasy (Hustler TV)'
when channel_desc in('CTV2T','TCTV2') then 'CTV Two Toronto'
when channel_desc in('FAIRW') then 'Fairchild Television West'
when channel_desc in('FAITH') then 'FAITH'
when channel_desc in('FAMHD','FAMLY','FAMW') then 'Family'
when channel_desc in('FASHN') then 'Fashion Television'
when channel_desc in('FEVA') then 'Feva TV'
when channel_desc in('DISC','DISCH') then 'Discovery'
when channel_desc in('FIGHT') then 'Fight Network'
when channel_desc in('FILMY') then 'FILMY'
when channel_desc in('FNHD') then 'Fight Network HD'
when channel_desc in('FNTSY') then 'FNTSY Sports Network'
when channel_desc in('SCI','SCIHD') then 'Discovery Science'
when channel_desc in('FOXNW') then 'FOX News Channel'
when channel_desc in('FOXSR') then 'FOX Sports Racing'
when channel_desc in('FPTV') then 'FPTV-SICi'
when channel_desc in('FREE') then 'FREE'
when channel_desc in('FREE1') then 'FREE1'
when channel_desc in('FRESH') then '953 Fresh FM'
when channel_desc in('FST4K') then 'Festival 4K'
when channel_desc in('FTBHD') then 'FTBHD'
when channel_desc in('FTV') then 'Filipino TV'
when channel_desc in('doc','docHD') then 'Documentary Channel'
when channel_desc in('FUJTV') then 'Fujian Straits TV'
when channel_desc in('FUTUR') then 'Future TV International'
when channel_desc in('DTORH','DTOUR') then 'DTOUR'
when channel_desc in('E!','E!HD') then 'E!'
when channel_desc in('FYI') then 'FYI'
when channel_desc in('GAME') then 'GameTV'
when channel_desc in('GEOTV') then 'GEO TV'
when channel_desc in('GLB-C') then 'Global Calgary (CICT)'
when channel_desc in('FAIR','FAIR.') then 'Fairchild Television East'
when channel_desc in('FTV2','FTV2H') then 'Fairchild TV 2'
when channel_desc in('GLOBO') then 'GLOBO'
when channel_desc in('GMALF') then 'GMA Life TV'
when channel_desc in('GMANW') then 'GMA News TV'
when channel_desc in('GMATV') then 'GMA Pinoy TV'
when channel_desc in('GMPLS') then 'GMPLS'
when channel_desc in('FAMJR','FJHD') then 'FamilyJr'
when channel_desc in('GRKCN') then 'Greek Cinema'
when channel_desc in('GSN') then 'GSN (Game Show Network)'
when channel_desc in('GUAND') then 'Guangdong Southern TV (TVS)'
when channel_desc in('GUETV') then 'GUETV'
when channel_desc in('GUIDE') then 'TV Listings'
when channel_desc in('GUSTO','GUSTH') then 'GUSTO'
when channel_desc in('FPC','HDFPC') then 'Fireplace Channel'
when channel_desc in('FOOD','HFOOD') then 'FOOD'
when channel_desc in('HDFOX','WUTV') then 'FOX Buffalo (WUTV)'
when channel_desc in('HFOXW','KCPQ') then 'FOX Seattle (KCPQ)'
when channel_desc in('HBOC1') then 'HBO Canada 1'
when channel_desc in('HBOC2') then 'HBO Canada 2'
when channel_desc in('HCADN') then 'HPItv Canada'
when channel_desc in('FX','FXHD') then 'FX'
when channel_desc in('HCTVV') then 'CTV HD British Columbia'
when channel_desc in('FXX','FXXHD') then 'FXX'
when channel_desc in('SCGNX','SCHDG') then 'GINX Esports TV Canada'
when channel_desc in('HD299') then 'HD299'
when channel_desc in('GL-BC','HDGBC') then 'Global BC (CHAN)'
when channel_desc in('HDCBC') then 'CBC'
when channel_desc in('GLOB','HDGLB') then 'Global Toronto (CIII)'
when channel_desc in('GLFHD','GOLF') then 'Golf Channel'
when channel_desc in('H2','H2HD') then 'H2'
when channel_desc in('HBO1','HBO1S') then 'HBO1'
when channel_desc in('HBO2','HBO2S') then 'HBO2'
when channel_desc in('HGTV','HHGTV') then 'HGTV'
when channel_desc in('FHIST','HISHD') then 'Historia'
when channel_desc in('HDHIS','HIST') then 'History TV'
when channel_desc in('HLN','HLNHD') then 'HLN'
when channel_desc in('ARTHD','ARTV') then 'ICI ARTV'
when channel_desc in('RDI','RDIHD') then 'ICI RDI'
when channel_desc in('ID','IDHD') then 'Investigation Discovery'
when channel_desc in('HKTLA','KTLA') then 'KTLA Los Angeles'
when channel_desc in('HDLTV','LEAFS') then 'Leafs Nation Network'
when channel_desc in('LTIME','LTMHD') then 'LifetimeOD'
when channel_desc in('HDS26') then 'HDS26'
when channel_desc in('HDS27') then 'HDS27'
when channel_desc in('HDS28') then 'HDS28'
when channel_desc in('HDS29') then 'HDS29'
when channel_desc in('SEC01','SEC1') then 'Lobby Watch Channel'
when channel_desc in('MLBHD','MLBN') then 'MLB Network'
when channel_desc in('HDMTM','MTIME') then 'MovieTime'
when channel_desc in('MTV','MTVHD') then 'MTV'
when channel_desc in('HMUCH','MUCH') then 'MUCH'
when channel_desc in('NASA','NASHD') then 'NASA TV'
when channel_desc in('NGW','NGWHD') then 'Nat Geo Wild'
when channel_desc in('HIFI') then 'HIFI'
when channel_desc in('HINTL') then 'HPItv International'
when channel_desc in('HDNAT','NATG') then 'Nat Geo'
when channel_desc in('NBAHD','NBATV') then 'NBA TV Canada'
when channel_desc in('HDNBC','WGRZ') then 'NBC Buffalo (WGRZ)'
when channel_desc in('HNBCW','KING') then 'NBC Seattle (KING)'
when channel_desc in('HPITV') then 'HPItv (HorsePlayer Interactive)'
when channel_desc in('NFLHD','NFLNT') then 'NFL'
when channel_desc in('HS00','00HSD') then 'HS00s'
when channel_desc in('HS70','70HSD') then 'HS70s'
when channel_desc in('HS80','80HSD') then 'HS80s'
when channel_desc in('HS90','90HSD') then 'HS90s'
when channel_desc in('HUMTV') then 'Hum TV'
when channel_desc in('HUNAN') then 'Hunan Satellite TV (HTV)'
when channel_desc in('HWEST') then 'HPItv West'
when channel_desc in('NICK','NICKH') then 'Nick'
when channel_desc in('I2DAY') then 'I2DAY'
when channel_desc in('IBAN') then 'Channel-I'
when channel_desc in('IBCT') then 'IBCT'
when channel_desc in('ICC1') then 'ICC1'
when channel_desc in('ICCH') then 'ICCH'
when channel_desc in('HDOM1','OMNI1') then 'OMNI.1'
when channel_desc in('IFC') then 'Independent Film Channel Canada'
when channel_desc in('INH1') then 'In House Channel 1'
when channel_desc in('INH2') then 'In House Channel 2'
when channel_desc in('INH3') then 'In House Channel 3'
when channel_desc in('INH4') then 'In House Channel 4'
when channel_desc in('INH5') then 'In House Channel 5'
when channel_desc in('ISRLI') then 'The Israeli Network'
when channel_desc in('ITALY') then 'Mediaset Italia'
when channel_desc in('ITVN') then 'iTVN'
when channel_desc in('JAPAN') then 'TV Japan'
when channel_desc in('JAYA') then 'ATN Jaya TV'
when channel_desc in('JIC') then 'Jiangsu International TV Channel'
when channel_desc in('JOY10') then 'Joytv 10'
when channel_desc in('KAP5') then 'Kapatid TV5'
when channel_desc in('HDOM2','OMNI2') then 'OMNI.2'
when channel_desc in('KICX') then 'KICX FM 106'
when channel_desc in('KIDS') then 'TV Mix - Kids'
when channel_desc in('OUTHD','OUTTV') then 'OUTTV'
when channel_desc in('KISS') then 'KISS 92.5 Toronto'
when channel_desc in('OWN','OWNHD') then 'OWN'
when channel_desc in('KTV') then 'KTV'
when channel_desc in('HDPBS','WNED') then 'PBS Buffalo (WNED)'
when channel_desc in('LEGPQ') then 'Le Canal de Assemblée Nationale du Québec'
when channel_desc in('LIFE') then 'LIFE'
when channel_desc in('LOCAL') then 'LOCAL'
when channel_desc in('LONDN') then 'LONDN'
when channel_desc in('LSTTV') then 'LS Times'
when channel_desc in('HPBSW','KCTS') then 'PBS Seattle (KCTS)'
when channel_desc in('LW1') then 'Lobby Watch Channel 1'
when channel_desc in('LW2') then 'Lobby Watch Channel 2'
when channel_desc in('LW3') then 'Lobby Watch Channel 3'
when channel_desc in('LW4') then 'Lobby Watch Channel 4'
when channel_desc in('LW5') then 'LW5'
when channel_desc in('LW6') then 'LW6'
when channel_desc in('LW7') then 'LW7'
when channel_desc in('LW8') then 'LW8'
when channel_desc in('M4K') then '4K Movies'
when channel_desc in('MAFLM') then 'Melody Aflam'
when channel_desc in('MAKFL') then 'Makeful'
when channel_desc in('MAX') then 'MAX'
when channel_desc in('MDRAM') then 'Melody Drama'
when channel_desc in('MEGA') then 'MEGA Cosmos'
when channel_desc in('METE') then 'MétéoMédia'
when channel_desc in('METEO') then 'METEO'
when channel_desc in('MHITS') then 'Melody Hits'
when channel_desc in('MIDL') then 'MIDL'
when channel_desc in('WPCH','WPCHD') then 'Peachtree TV'
when channel_desc in('MPLS','MPLS.') then 'MusiquePlus'
when channel_desc in('MSNBC') then 'MSNBC'
when channel_desc in('PLBY','PLBYH') then 'Playboy TV'
when channel_desc in('MTV2') then 'MTV2'
when channel_desc in('RDS','RDSH.','RDSHD','RDS.') then 'RDS'
when channel_desc in('MTVIN') then 'MTV India'
when channel_desc in('MULTI') then 'Multicultural Free Preview'
when channel_desc in('MUSIK') then 'ARY Musik'
when channel_desc in('NAHAR') then 'Al Nahar'
when channel_desc in('HDRSI','RDSI','RDSI.','RDISH','HRDSI','RDSIH') then 'RDS Info'
when channel_desc in('NATUR') then 'Love Nature'
when channel_desc in('NDTGT') then 'NDTV Good Times'
when channel_desc in('NDTV') then 'ATN NDTV 24x7'
when channel_desc in('NEWS') then 'TV Mix - News'
when channel_desc in('NFL1') then 'NFL1'
when channel_desc in('NFL10') then 'NFL10'
when channel_desc in('NFL2') then 'NFL2'
when channel_desc in('NFL3') then 'NFL3'
when channel_desc in('NFL4') then 'NFL4'
when channel_desc in('NFL5') then 'NFL5'
when channel_desc in('NFL6') then 'NFL6'
when channel_desc in('NFL7') then 'NFL7'
when channel_desc in('NFL8') then 'NFL8'
when channel_desc in('NFL9') then 'NFL9'
when channel_desc in('GRID','GRIDH') then 'Rogers Grid?'
when channel_desc in('NINOS') then 'TeleNinos'
when channel_desc in('NOW4K') then 'NOW4K'
when channel_desc in('NTD') then 'NTD (New Tang Dynasty Television)'
when channel_desc in('NTVB') then 'NTV Bangla'
when channel_desc in('ODSY') then 'Odyssey'
when channel_desc in('OLEG') then 'Ontario Legislature'
when channel_desc in('OLEGT') then 'OLEGT'
when channel_desc in('OLN','OLNHD') then 'Outdoor Life Network'
when channel_desc in('OMNBC') then 'OMNI BC'
when channel_desc in('OMNI') then 'OMNI'
when channel_desc in('ONE') then 'One: Get Fit'
when channel_desc in('ORILL') then 'ORILL'
when channel_desc in('OWENS') then 'OWENS'
when channel_desc in('PAR','PARHD') then 'PARAMOUNTTV'
when channel_desc in('PENT') then 'Penthouse TV'
when channel_desc in('PHNIX') then 'Phoenix North America Chinese'
when channel_desc in('PLYMN') then 'Playmen TV'
when channel_desc in('PR1') then 'Polskie Radio 1 (Polish)'
when channel_desc in('PR3') then 'Polskie Radio 3 (Polish)'
when channel_desc in('PREVU') then 'Pay Per View Preview Channel'
when channel_desc in('PRIME') then 'Prime Asia TV'
when channel_desc in('PRISE') then 'Prise 2'
when channel_desc in('PRO7') then 'ProSiebenSat.1 Welt'
when channel_desc in('PTC') then 'PTC Punjabi Canada'
when channel_desc in('PUNJ+') then 'ATN Punjabi Plus'
when channel_desc in('PUNJA') then 'Alpha ETC Punjabi'
when channel_desc in('Q107') then 'Q107 Toronto'
when channel_desc in('QTV') then 'ARY QTV'
when channel_desc in('RAI') then 'RAI italia (Italian)'
when channel_desc in('RAINW') then 'RAINews'
when channel_desc in('RAIWP') then 'RAI World Premium'
when channel_desc in('RBTI') then 'RBTI'
when channel_desc in('RDXXX') then 'RED HOT TV'
when channel_desc in('REC') then 'Real Estate Channel'
when channel_desc in('RECRD') then 'Record TV'
when channel_desc in('REW') then 'Rewind'
when channel_desc in('RISHT') then 'ATN Rishtey'
when channel_desc in('RITMO') then 'Ritmoson Latino Channel'
when channel_desc in('ROD') then 'ROD'
when channel_desc in('ROGTV') then 'ROGTV'
when channel_desc in('RT') then 'RT'
when channel_desc in('RTNAC') then 'Rotana Cinema'
when channel_desc in('RTPi') then 'RTPi'
when channel_desc in('RTRP') then 'RTR Planeta'
when channel_desc in('RTV') then 'Rogers TV'
when channel_desc in('RTV22') then 'RTV22'
when channel_desc in('RTVBA') then 'RTVBA'
when channel_desc in('RTVBR') then 'Rogers TV HD (Barrie)'
when channel_desc in('RTVDH') then 'Rogers TV HD (Durham)'
when channel_desc in('RTVHC') then 'Rogers TV HD (Collingwood)'
when channel_desc in('RTVHG') then 'Rogers TV HD (Guelph)'
when channel_desc in('RTVHL') then 'Rogers TV HD (London)'
when channel_desc in('RTVHO') then 'Rogers TV HD (Ottawa)'
when channel_desc in('RTVHS') then 'Rogers TV HD (St. Thomas)'
when channel_desc in('RTVi') then 'RTVi'
when channel_desc in('RTVKW') then 'RTV Keswick HD'
when channel_desc in('RTVOS') then 'Rogers TV HD (Owen Sound)'
when channel_desc in('RTVOV') then 'RTV Orangeville HD'
when channel_desc in('RTVST') then 'RTVST'
when channel_desc in('RTVWA') then 'RTVWA'
when channel_desc in('RTVWS') then 'RTV Woodstock HD'
when channel_desc in('S1') then 'Super Écran 1'
when channel_desc in('S2') then 'Super Écran 2'
when channel_desc in('S3') then 'Super Écran 3'
when channel_desc in('S4') then 'Super Écran 4'
when channel_desc in('SABTV') then 'SAB TV'
when channel_desc in('SAHRA') then 'Sahara One'
when channel_desc in('SALT') then 'Salt + Light Television'
when channel_desc in('SBTN') then 'Saigon Broadcasting TV Network'
when channel_desc in('SCFUS','SCHDF') then 'Super Channel Fuse'
when channel_desc in('SCIFI') then 'SCIFI'
when channel_desc in('SD299') then 'SD299'
when channel_desc in('SEC02') then 'Lobby Watch Channel Ottawa'
when channel_desc in('SEC03') then 'SEC03'
when channel_desc in('SEHD') then 'Super Écran HD'
when channel_desc in('SETMX') then 'Set MAX'
when channel_desc in('SICNO') then 'SIC Noticias'
when channel_desc in('SIKH') then 'ATN Sikh Channel'
when channel_desc in('SILVR') then 'Silver Screen Classics'
when channel_desc in('SKYNA') then 'Sky News Arabic'
when channel_desc in('SM1') then 'Stingray Classic Rock'
when channel_desc in('SM10') then 'Stingray Jukebox Oldies'
when channel_desc in('SM11') then 'Stingray Remember the 80s'
when channel_desc in('SM12') then 'Stingray Special FR'
when channel_desc in('SM13') then 'Stingray Franco Pop'
when channel_desc in('SM14') then 'Stingray Nostalgie'
when channel_desc in('SM15') then 'Stingray Flashback 70s'
when channel_desc in('SM16') then 'Stingray Special EN'
when channel_desc in('SM17') then 'Stingray Pop Classics'
when channel_desc in('SM18') then 'Stingray The Blues'
when channel_desc in('SM19') then 'Stingray Baroque'
when channel_desc in('SM2') then 'Stingray Rock Alternative'
when channel_desc in('SM20') then 'Stingray Chamber Music'
when channel_desc in('SM21') then 'Stingray The Chill Lounge'
when channel_desc in('SM22') then 'Stingray The Spa'
when channel_desc in('SM23') then 'Stingray Around the World'
when channel_desc in('SM24') then 'Stingray Folk Roots'
when channel_desc in('SM25') then 'Stingray Nature'
when channel_desc in('SM26') then 'Stingray Classic Masters'
when channel_desc in('SM27') then 'Stingray Jazz Masters'
when channel_desc in('SM28') then 'Stingray Maximum Party'
when channel_desc in('SM29') then 'Stingray Kids Stuff'
when channel_desc in('SM3') then 'Stingray Adult Alternative'
when channel_desc in('SM30') then 'Stingray MoussesMusique'
when channel_desc in('SM31') then 'Stingray Top Détente'
when channel_desc in('SM33') then 'Stingray Urban Beat'
when channel_desc in('SM34') then 'Stingray Franco Retro'
when channel_desc in('SM35') then 'Stingray Big Band'
when channel_desc in('SM36') then 'Stingray Rock'
when channel_desc in('SM37') then 'Stingray Easy Listening'
when channel_desc in('SM38') then 'Stingray Eclectic Electronic'
when channel_desc in('SM39') then 'Stingray The Light'
when channel_desc in('SM4') then 'Stingray Country Classics'
when channel_desc in('SM40') then 'Stingray Smooth Jazz'
when channel_desc in('SM41') then 'Stingray Hindi Gold'
when channel_desc in('SM42') then 'Stingray Bollywood Hits'
when channel_desc in('SM43') then 'Stingray Sounds of South India'
when channel_desc in('SM44') then 'Stingray Classical India'
when channel_desc in('SM45') then 'Stingray Punjabi'
when channel_desc in('SM46') then 'Stingray Guangdong'
when channel_desc in('SM47') then 'Stingray Mando Popular'
when channel_desc in('SM48') then 'Stingray The Asian Flavour'
when channel_desc in('SM49') then 'Stingray Tagalog'
when channel_desc in('SM5') then 'Stingray Franco Country'
when channel_desc in('SM50') then 'Stingray Arabic'
when channel_desc in('SM51') then 'Stingray Farsi'
when channel_desc in('SM52') then 'Stingray Latin'
when channel_desc in('SM53') then 'Stingray Latino Tropical'
when channel_desc in('SM54') then 'Stingray Retro Latino'
when channel_desc in('SM55') then 'Stingray Latino Tejano'
when channel_desc in('SM56') then 'Stingray Regional Mexican'
when channel_desc in('SM57') then 'Stingray Canadian Indie'
when channel_desc in('SM58') then 'Stingray Canadiana'
when channel_desc in('SM59') then 'Stingray Franco Attitude'
when channel_desc in('SM6') then 'Stingray Dance Clubbin'
when channel_desc in('SM60') then 'Stingray Opera Plus'
when channel_desc in('SM61') then 'Stingray VIBE'
when channel_desc in('SM62') then 'Stingray LOUD'
when channel_desc in('SM63') then 'Juice'
when channel_desc in('SM64') then 'Stingray RETRO'
when channel_desc in('SM7') then 'Stingray Hit List'
when channel_desc in('SM76') then 'Stingray Riddim'
when channel_desc in('SM8') then 'Stingray Hot Country'
when channel_desc in('SM9') then 'Stingray Pop Adult'
when channel_desc in('SMITH') then 'Smithsonian Channel'
when channel_desc in('SMIX') then 'ATN Sony Mix'
when channel_desc in('SONY') then 'SET Asia'
when channel_desc in('S+HD','SPLUS') then 'Séries+'
when channel_desc in('HDSHO','SHOW') then 'Showcase'
when channel_desc in('SLICE','SLIHD') then 'Slice'
when channel_desc in('SPACE','SPCHD') then 'Space'
when channel_desc in('SPN','SPNHD') then 'Sportsnews'
when channel_desc in('STZ1H','STZ1S') then 'STARZ 1'
when channel_desc in('STZ2H','STZ2S') then 'STARZ 2'
when channel_desc in('SPRTS') then 'TV Mix - Sports'
when channel_desc in('SCHDH','SCHRT') then 'Super Channel Heart and Home'
when channel_desc in('SSTV') then 'SSTV'
when channel_desc in('STARG') then 'Star India Gold'
when channel_desc in('SCHDV','SCVLT') then 'Super Channel Vault'
when channel_desc in('T+EHD','T+ESD') then 'T and E'
when channel_desc in('SUNTA') then 'Sun TV'
when channel_desc in('SVOIR') then 'Canal Savoir'
when channel_desc in('TCM','TCMHD') then 'TCM'
when channel_desc in('TALNT') then 'Talentvision (Chinese-Mandarin)'
when channel_desc in('CIVM','TQHD') then 'Télé-Québec (CIVM)'
when channel_desc in('TET') then 'Tamil Entertainment Television (TET)'
when channel_desc in('TFC') then 'TFC (The Filipino Channel)'
when channel_desc in('CIVO','TQHHD') then 'Télé-Québec Hull (CIVO)'
when channel_desc in('TG24') then 'SKY TG24 Canada'
when channel_desc in('TILL') then 'TILL'
when channel_desc in('TIMES') then 'Times Now'
when channel_desc in('TOON','TTHD') then 'TELETOON'
when channel_desc in('TFO','TFOHD') then 'TFO (CHLF)'
when channel_desc in('TLNOV') then 'TL Novelas América'
when channel_desc in('TLNW') then 'Telelatino (TLN - West)'
when channel_desc in('TONE') then 'Tamil One'
when channel_desc in('CMDY','CMDYH','CMDYW') then 'ComedyNetwork'
when channel_desc in('TOONF') then 'TÉLÉTOON (French)'
when channel_desc in('TOONW') then 'TELETOON (West)'
when channel_desc in('TWNHD','WN') then 'The Weather Network'
when channel_desc in('TRAXP') then 'Travelxp HD'
when channel_desc in('TLC','TLCHD') then 'TLC'
when channel_desc in('TLN','TLNHD') then 'TLN'
when channel_desc in('ENC1','ENC2') then 'TMN Encore'
when channel_desc in('TREE','TREEH') then 'TREEHOUSE'
when channel_desc in('TSC','TSCHD') then 'TSC'
when channel_desc in('TVAHD') then 'TVA HD'
when channel_desc in('TVO','TVOHD') then 'TV Ontario'
when channel_desc in('TV5','TV5HD') then 'TV5'
when channel_desc in('TVCHL') then 'TV Chile'
when channel_desc in('TVCI') then 'TVCI'
when channel_desc in('TVI') then 'Tamil Vision'
when channel_desc in('TVIPT') then 'TVI'
when channel_desc in('CHOT','HTVAH') then 'TVA Hull (CHOT)'
when channel_desc in('TVP') then 'TV Polonia'
when channel_desc in('TVR23') then 'TVR23'
when channel_desc in('TVRHO') then 'TV Rogers HD (Ottawa)'
when channel_desc in('TWNBH') then 'TWN HD (Barrie)'
when channel_desc in('TWNHB') then 'TWN HD (Brampton)'
when channel_desc in('TWNHE') then 'TWN HD (Etobicoke)'
when channel_desc in('TWNHG') then 'TWNHG'
when channel_desc in('TWNHK') then 'TWN HD (Kitchener)'
when channel_desc in('TWNHL') then 'TWN HD (London)'
when channel_desc in('TWNHN') then 'TWN HD (NwmktAurora)'
when channel_desc in('TWNHO') then 'TWN HD (Ottawa)'
when channel_desc in('TWNHP') then 'TWN HD (Pickering)'
when channel_desc in('TWNHR') then 'TWN HD (Richmond)'
when channel_desc in('TWNHS') then 'TWN HD (Scarborough)'
when channel_desc in('TWNHT') then 'TWN HD (Toronto)'
when channel_desc in('TWNOH') then 'TWN HD (Oshawa)'
when channel_desc in('UNIVS') then 'Univision Canada'
when channel_desc in('URDU') then 'ATN Urdu'
when channel_desc in('UTVMI') then 'UTV Movies International'
when channel_desc in('VGNTV') then 'VGNTV'
when channel_desc in('VIS') then 'VisionTV'
when channel_desc in('UNIS','UNISH') then 'Unis TV'
when channel_desc in('VRTCL') then 'Vertical TV'
when channel_desc in('V','V HD') then 'V Hull (CFGS)'
when channel_desc in('WBFO') then 'WBFO 88.7FM'
when channel_desc in('WDIV') then 'NBC Detroit (WDIV)'
when channel_desc in('VRAK','VRKHD') then 'VRAK.TV'
when channel_desc in('WILD') then 'Wild TV'
when channel_desc in('W','W HD') then 'W'
when channel_desc in('WJBK') then 'FOX Detroit (WJBK)'
when channel_desc in('HDWGN','WGN') then 'WGN Chicago (CW9)'
when channel_desc in('WLDHD') then 'Wild TV HD'
when channel_desc in('WMNB') then 'WMNB (Russian)'
when channel_desc in('WIN','WIN ') then 'WIN Caribbean'
when channel_desc in('WNEDF') then 'WNED-FM'
when channel_desc in('WNLO') then 'WNLO Buffalo (CW23)'
when channel_desc in('WNYO') then 'WNYO Buffalo (MyNetworkTV)'
when channel_desc in('WOOD') then 'WOOD'
when channel_desc in('WOWHD','WOWtv') then 'WOWtv'
when channel_desc in('WPBS') then 'PBS Watertown (WPBS)'
when channel_desc in('HWPIX','WPIX') then 'WPIX New York (CW11)'
when channel_desc in('WQLN') then 'PBS Erie (WQLN)'
when channel_desc in('HWSBK','WSBK') then 'WSBK Boston (TV38)'
when channel_desc in('WTVS') then 'PBS Detroit (WTVS)'
when channel_desc in('WUAB') then 'WUAB Cleveland (My43, WUAB)'
when channel_desc in('WW') then 'W Network (West)'
when channel_desc in('WWE') then 'WWE Network'
when channel_desc in('WWJ') then 'CBS Detroit (WWJ)'
when channel_desc in('WXYZ') then 'ABC Detroit (WXYZ)'
when channel_desc in('X4K') then 'XITE 4K'
when channel_desc in('Y') then 'Channel Y'
when channel_desc in('YESHD','YESTV') then 'YES TV'
when channel_desc in('YHALA') then 'Ya Hala '
when channel_desc in('YWHD','YWTW') then 'Your World This Week'
when channel_desc in('YTV','YTVHD','YTVW') then 'YTV'
when channel_desc in('ZAUQ') then 'ARY Zauq'
when channel_desc in('ZCINC') then 'Zee Cinema Canada'
when channel_desc in('ZEE') then 'Zee Bollywood HD'
when channel_desc in('ZEEHD','ZEETV') then 'ZEE TV Canada'
when channel_desc in('ZESHD','ZESTE') then 'Zeste'
when channel_desc in('ZHD','Ztele') then 'Ztélé'
when channel_desc in('ZING') then 'Zing'
when channel_desc in('ZOOM') then 'Zoom'
when channel_desc in('ZTAM') then 'Zee Tamizh'
when channel_desc in (
'HD10',
'HD20',
'HDS11',
'HDS12',
'HDS13',
'HDS14',
'HDS15',
'HDS16',
'HDS17',
'HDS18',
'HDS19',
'HDS20',
'HDS21',
'HDS22',
'HDS23',
'HDS24',
'HDSP1',
'HDSP2',
'HDSP3',
'HDSP4',
'HDSP5',
'HDSP6',
'HDSP7',
'HDSP8',
'HDSP9',
'SP1',
'SP21',
'SPO1',
'SPO10',
'SPO11',
'SPO12',
'SPO13',
'SPO14',
'SPO15',
'SPO16',
'SPO17',
'SPO18',
'SPO19',
'SPO2',
'SPO20',
'SPO21',
'SPO23',
'SPO24',
'SPO25',
'SPO26',
'SPO27',
'SPO28',
'SPO29',
'SPO3',
'SPO30',
'SPO31',
'SPO32',
'SPO33',
'SPO34',
'SPO35',
'SPO36',
'SPO37',
'SPO38',
'SPO39',
'SPO4',
'SPO40',
'SPO41',
'SPO5',
'SPO6',
'SPO7',
'SPO8',
'SPO9',
'SSPHD',
'SSPSD') then 'Rogers Super Sports Pak'
when channel_desc in (
'360HD',
'SN1',
'SN14K',
'SN1HD',
'SN360',
'SN4K',
'SN4K.',
'SNE',
'SNE.',
'SNEH.',
'SNEHD',
'SNO',
'SNOH.',
'SNOHD',
'SNP',
'SNPHD',
'SNW',
'SNWH1',
'SNWH2',
'SNWH3',
'SNWH4',
'SNWHD',
'SNWL',
'SNWLH',
'SNWP1',
'SNWP2',
'SNWP3',
'SNWP4') then 'Sportsnet'
when channel_desc in (
'TSP2H',
'TSP3H',
'TSPH.',
'TSPHD',
'TVAS.',
'TVAS2',
'TVAS3',
'TVASP') then 'TVA Sports'
WHEN CHANNEL_DESC IN (
'TSN 4',
'TSN 5',
'TSN1' ,
'TSN1H',
'TSN2' ,
'TSN2.',
'TSN2H',
'TSN2K',
'TSN3' ,
'TSN3H',
'TSN4' ,
'TSN4H',
'TSN4K',
'TSN5' ,
'TSN5H',
'TSN5K',
'TSNH2',
'TSNH4',
'TSNH5') THEN 'TSN'
else channel_desc
END as channel_grouping,substring(cast(start_date as string),1,7) as part_month
from stb_viewership.viewership_fact 
WHERE start_date < '2023-09-30'
AND duration_min >= 3
AND duration_min <= 720
AND CAN is not null
AND 
(lower(source_filename) LIKE '%atl%' 
OR lower(source_filename)  LIKE '%on%' 
OR lower(source_filename)  LIKE '%sc%' )
GROUP BY 1,2,3,7,start_date
order by 1,2,3,7,start_date;
--select * from FH_DTV_View_July_2023;
'''
print(dtv_viewership_qry)
dtv_viewership1 = spark.sql(dtv_viewership_qry)
dtv_viewership = dtv_viewership1.withColumnRenamed("mins","totalmins").withColumnRenamed("channel_grouping","channel_name").withColumn("az_insert_ts",current_timestamp()).withColumn("az_update_ts",current_timestamp())
display(dtv_viewership)

# COMMAND ----------

parts = dtv_viewership.select("part_month").distinct().orderBy("part_month").rdd.map(lambda x : x[0]).collect()
print(parts)

# COMMAND ----------

processed = spark.read.table("stb_viewership.DTV_Test").select("part_month").distinct()
processed_dates = processed.select("part_month").distinct().orderBy("part_month").rdd.map(lambda x : x[0]).collect()
print(processed_dates)

# COMMAND ----------

if any(x in parts for x in processed_dates):
  currMaxDeltaColVal = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
  srcRecCount = str(spark.read.table(tgtTableName).count())
  dbutils.notebook.exit({"Exit Message":"Data for the given month is already processed.Hence exiting the notebook","srcRecCount":srcRecCount,"currMaxDeltaColVal": currMaxDeltaColVal})

# COMMAND ----------

counter=0
primaryKey = "year,month,region,viewers,tuners,totalmins,channel_name,part_month"
while (counter < len(parts)):
  parts1 = parts[counter]
  part_month = parts1[0:8]
  print(part_month)
  datafilter = f"part_month = '{part_month}'"
  dtv_viewership.createOrReplaceTempView("vw_dtv_viewership_delta")
  print(datafilter)
  
  insDF = spark.sql(f"select * from vw_dtv_viewership_delta where {datafilter}")
  
  insDF.write.mode("overwrite").format("delta").option("replaceWhere", datafilter).insertInto(tgtTableName)

  counter = counter + 1

# COMMAND ----------

currMaxDeltaColVal = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
srcRecCount = str(spark.read.table(tgtTableName).count())

# COMMAND ----------

dbutils.notebook.exit({"srcRecCount":srcRecCount,"currMaxDeltaColVal": currMaxDeltaColVal})

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from  stb_viewership.dtv_test --where part_month = '2023-07'
