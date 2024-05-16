# Databricks notebook source
# MAGIC %md
# MAGIC Configuration parameters

# COMMAND ----------

# DBTITLE 1,API Authentication
# If you want to run this notebook yourself, you need to create a Databricks personal access token,
# store it using our secrets API, and pass it in through the Spark config, such as this:
# spark.pat_token {{secrets/query_history_etl/user}}, or Azure Keyvault.

#Databricks secrets API
#AUTH_HEADER = {"Authorization" : "Bearer " + spark.conf.get("spark.pat_token")}
#Azure KeyVault
AUTH_HEADER = {"Authorization" : "Bearer " + dbutils.secrets.get(scope = "hls_demo_secret_scope", key = "fieldeng-pat-token")}
#Naughty way
#AUTH_HEADER = {"Authorization" : "Bearer " + "<pat_token>"}
print(AUTH_HEADER)
# Expect: {'Authorization': 'Bearer [REDACTED]'}

# COMMAND ----------

# DBTITLE 1,Runtime parameters
DATABASE_NAME = "finops"
SCHEMA_NAME = "system_lookups"
# Lists all objects that a user has manager permissions on.# 
WORKSPACE_HOST = 'https://adb-984752964297111.11.azuredatabricks.net'

# COMMAND ----------

# DBTITLE 1,dbu discounts
# Discount for each month, can skip
# Be sure to include a distant end date
DBU_DISCOUNTS = {
  "01-2020":"0.235",
  "01-2030":"0.235"
}

# COMMAND ----------

# DBTITLE 1,cost markup
# Markup for each month can skp
# Be sure to include a distant end date
INFRA_MARKUPS = {
  "01-2020":"0.65",
  "02-2024":"0.55",
  "03-2024":"0.55",
  "01-2030":"0.55"
}

# COMMAND ----------

# DBTITLE 1,Workspace Names
WORKSPACE_NAMES = {
"6024433575559853":"WKSP59853",
"6058900950367176":"WKSP67176",
"2574677666339144":"WKSP39144",
"7332616426171090":"WKSP71090",
"4522126718558142":"WKSP58142",
"1504693088581704":"WKSP81704",
"6417907769725610":"WKSP25610",
"3164338161553697":"WKSP53697",
"6479767029813236":"WKSP13236",
"4052619945798537":"WKSP98537",
"3298326044198364":"WKSP98364",
"5589724225482834":"WKSP82834",
"8494954704168407":"WKSP68407",
"1758501977368780":"WKSP68780",
"8551086942772682":"WKSP72682",
"2295939694691536":"WKSP91536",
"164447565005534":"WKSP05534",
"2860636321068589":"WKSP68589",
"3939299280266473":"WKSP66473",
"5021572262110760":"WKSP10760",
"6042569476650449":"WKSP50449",
"8697961124191164":"WKSP91164",
"5918241181427346":"WKSP27346",
"6583894373483804":"WKSP83804",
"1092069688382665":"WKSP82665",
"2526066434561540":"WKSP61540",
"2138805328847975":"WKSP47975",
"8097797545125083":"WKSP25083",
"2290777133481849":"WKSP81849",
"1763733720934929":"WKSP34929",
"1292723388527512":"WKSP27512",
"102179013649963":"WKSP49963",
"7938161883653767":"WKSP53767",
"5341885230731770":"WKSP31770",
"4887479028814153":"WKSP14153",
"5120982421848975":"WKSP48975",
"3083911735432218":"WKSP32218",
"2409798316028604":"WKSP28604",
"4551890975194101":"WKSP94101",
"2435634869968544":"WKSP68544",
"6980887430007370":"WKSP07370",
"5420674832747555":"WKSP47555",
"7483925004455582":"WKSP55582",
"3438334211944774":"WKSP44774",
"6402838246993234":"WKSP93234",
"3101270908368075":"WKSP68075",
"6955458121956618":"WKSP56618",
"7748370821458396":"WKSP58396",
"7338663619064447":"WKSP64447",
"3144473413841570":"WKSP41570",
"3083718471940291":"WKSP40291",
"1835921190652375":"WKSP52375",
"6453876552782059":"WKSP82059",
"5072924269441774":"WKSP41774",
"8041580868257487":"WKSP57487",
"5730379358313227":"WKSP13227",
"5864016996602823":"WKSP02823",
"1693024673414830":"WKSP14830",
"977434407135772":"WKSP35772",
"2156901387044490":"WKSP44490",
"1024335989005427":"WKSP05427",
"7255343216264672":"WKSP64672",
"6764813965713299":"WKSP13299",
"468868648456330":"WKSP56330",
"7210300897147982":"WKSP47982",
"3409771225390170":"WKSP90170",
"4485980902162692":"WKSP62692",
"7538144878229740":"WKSP29740",
"3180815342576841":"WKSP76841",
"7226359427336997":"WKSP36997",
"4880129228634146":"WKSP34146",
"361426925668745":"WKSP68745",
"3450184585407536":"WKSP07536",
"2859578595274382":"WKSP74382",
"8714522172974099":"WKSP74099",
"2050695945626507":"WKSP26507",
"3418378506571437":"WKSP71437",
"7835420621338294":"WKSP38294",
"2428380558249112":"WKSP49112",
"8652781164032496":"WKSP32496",
"1328961954232377":"WKSP32377",
"8245268741408838":"WKSP08838",
"566733337217159":"WKSP17159",
"2829984661787189":"WKSP87189",
"1540989833392185":"WKSP92185",
"5064897208349953":"WKSP49953",
"1656642753785427":"WKSP85427",
"7517998191412840":"WKSP12840",
"3820595173453839":"WKSP53839",
"3055962936693113":"WKSP93113",
"4225909318435949":"WKSP35949",
"8549610791878758":"WKSP78758",
"3367042399445142":"WKSP45142",
"6317283689292176":"WKSP92176",
"3481004915357084":"WKSP57084",
"1599693813987979":"WKSP87979",
"4061164006854523":"WKSP54523",
"2714780438587199":"WKSP87199",
"7639267005201021":"WKSP01021",
"2784935097795705":"WKSP95705",
"8069751836307970":"WKSP07970",
"3360701225556151":"WKSP56151",
"9992913278663":"WKSP78663",
"7673939281804002":"WKSP04002",
"7440072970431467":"WKSP31467",
"5661010265361526":"WKSP61526",
"2097946849655833":"WKSP55833",
"8989174856283104":"WKSP83104",
"6212782030094992":"WKSP94992",
"6739957880028432":"WKSP28432",
"3464372833038054":"WKSP38054",
"3866011631731048":"WKSP31048",
"459962129828334":"WKSP28334",
"1709512232663319":"WKSP63319",
"1181494414619462":"WKSP19462",
"256827766187796":"WKSP87796",
"3307537946634527":"WKSP34527",
"2135334422787436":"WKSP87436",
"7384755941562252":"WKSP62252",
"4215341147994214":"WKSP94214",
"2499125262158313":"WKSP58313",
"2413493562262293":"WKSP62293",
"2433165646264587":"WKSP64587",
"8891857670932073":"WKSP32073",
"2402578647378195":"WKSP78195",
"1855084017998148":"WKSP98148",
"2517174839264498":"WKSP64498",
"1648909751048163":"WKSP48163",
"7415837690512475":"WKSP12475",
"3464202434711190":"WKSP11190",
"3850682965566023":"WKSP66023",
"7628566888585154":"WKSP85154",
"3693518721750537":"WKSP50537",
"49615766655221":"WKSP55221",
"6440753715400994":"WKSP00994",
"6959653494322738":"WKSP22738",
"7491147192189600":"WKSP89600",
"763019740922755":"WKSP22755",
"3501726460886606":"WKSP86606",
"5809742986594827":"WKSP94827",
"2704554918254528":"WKSP54528",
"8509947773089144":"WKSP89144",
"350782973030770":"WKSP30770",
"6650479735101543":"WKSP01543",
"7011889034403761":"WKSP03761",
"4081400383985196":"WKSP85196",
"7909534421021138":"WKSP21138",
"1411416365490081":"WKSP90081",
"654054599282512":"WKSP82512",
"6531161934228505":"WKSP28505",
"5434241424606867":"WKSP06867",
"7595662984774358":"WKSP74358",
"6803238274136648":"WKSP36648",
"7260115916726986":"WKSP26986",
"7821165808400161":"WKSP00161",
"2076521671881056":"WKSP81056",
"4622246536464924":"WKSP64924",
"6400243820477786":"WKSP77786",
"1840906908195430":"WKSP95430",
"2186844454500186":"WKSP00186",
"5305532140974507":"WKSP74507",
"6075770224669897":"WKSP69897",
"1867109538537497":"WKSP37497",
"2312822982598378":"WKSP98378",
"3807631632485287":"WKSP85287",
"1233459725410486":"WKSP10486",
"6599164319956696":"WKSP56696",
"1045986908780898":"WKSP80898",
"8876629969026717":"WKSP26717",
"3282315413398664":"WKSP98664",
"1355231926142695":"WKSP42695",
"147958763567251":"WKSP67251",
"7269283947178059":"WKSP78059",
"5248533068051179":"WKSP51179",
"2358407608911860":"WKSP11860",
"7380952450191803":"WKSP91803",
"2910138034503135":"WKSP03135",
"106235430671726":"WKSP71726",
"7129472518829356":"WKSP29356",
"58637995566999":"WKSP66999",
"2541733722036151":"WKSP36151",
"1179229660425454":"WKSP25454",
"5343834423590926":"WKSP90926",
"887071945425358":"WKSP25358",
"6073203130470537":"WKSP70537",
"4081889517933821":"WKSP33821",
"2890556830100440":"WKSP00440",
"4467341220514145":"WKSP14145",
"8711109166266353":"WKSP66353",
"5362147574520601":"WKSP20601",
"406318288470266":"WKSP70266",
"6276654338595209":"WKSP95209",
"4834225110635252":"WKSP35252",
"5373804284862353":"WKSP62353",
"5969622949626094":"WKSP26094",
"25175640427992":"WKSP27992",
"4140265294884438":"WKSP84438",
"1255009867300082":"WKSP00082",
"3635873547423038":"WKSP23038",
"2315607139980329":"WKSP80329",
"2589304842848547":"WKSP48547",
"7081433878496846":"WKSP96846",
"3603228194907977":"WKSP07977",
"1006268905948121":"WKSP48121",
"3526791425783092":"WKSP83092",
"6120542968195864":"WKSP95864",
"7935126702370661":"WKSP70661",
"8293066282205549":"WKSP05549",
"144562338803909":"WKSP03909",
"2196542718373551":"WKSP73551",
"1731866639310210":"WKSP10210",
"900140441508320":"WKSP08320",
"5030245195933557":"WKSP33557",
"4181344995636100":"WKSP36100",
"6573344999136127":"WKSP36127",
"4100145449670075":"WKSP70075",
"7384371457227396":"WKSP27396",
"2290419374598257":"WKSP98257",
"1034825557286978":"WKSP86978",
"3064928281582730":"WKSP82730",
"4593423501108947":"WKSP08947",
"7553575517810009":"WKSP10009",
"4213877156129387":"WKSP29387",
"8969286400017083":"WKSP17083",
"4432414056926734":"WKSP26734",
"8699117216178317":"WKSP78317",
"4247034115621306":"WKSP21306",
"1227721138324052":"WKSP24052",
"2698509223681136":"WKSP81136",
"5569308596049977":"WKSP49977",
"8752583164848723":"WKSP48723",
"5635596728267912":"WKSP67912",
"7744367634789941":"WKSP89941",
"8706611867193881":"WKSP93881",
"5226704691083619":"WKSP83619",
"3223469345491610":"WKSP91610",
"600342202279357":"WKSP79357",
"7281623853490395":"WKSP90395",
"7628792571421202":"WKSP21202",
"7493281886852642":"WKSP52642",
"6593548714944209":"WKSP44209",
"1356097079529509":"WKSP29509",
"1763917921201207":"WKSP01207",
"6866998694469682":"WKSP69682",
"6387357674564814":"WKSP64814",
"2586718869528124":"WKSP28124",
"2587552597389561":"WKSP89561",
"4157450669386484":"WKSP86484",
"8590162618558854":"WKSP58854",
"4562026214062884":"WKSP62884",
"6508744562641075":"WKSP41075",
"6196439975714496":"WKSP14496",
"3295413107393990":"WKSP93990",
"1075352391660530":"WKSP60530",
"5524270988312315":"WKSP12315",
"2998866575208694":"WKSP08694",
"4395249209562397":"WKSP62397",
"2826588025657021":"WKSP57021",
"4580283122340203":"WKSP40203",
"4904724370685557":"WKSP85557",
"5542934650220056":"WKSP20056",
"2344927648629615":"WKSP29615",
"2413569332683965":"WKSP83965",
"1207649392870807":"WKSP70807",
"3546555789454657":"WKSP54657",
"7575756612346860":"WKSP46860",
"4336676224892962":"WKSP92962",
"984500932282728":"WKSP82728",
"1831379400797234":"WKSP97234",
"4354247350364230":"WKSP64230",
"5204366089065245":"WKSP65245",
"3519373541998114":"WKSP98114",
"8713547057565165":"WKSP65165",
"845910300607100":"WKSP07100",
"2319045324119159":"WKSP19159",
"2644488219971018":"WKSP71018",
"7576039702303183":"WKSP03183",
"1030771517633244":"WKSP33244",
"1324247806228240":"WKSP28240",
"4866553791294211":"WKSP94211",
"3112411080242014":"WKSP42014",
"8866561792403075":"WKSP03075",
"4378376778898505":"WKSP98505",
"3272019941346163":"WKSP46163",
"6425625106767597":"WKSP67597",
"6846391555506245":"WKSP06245",
"8073984702147692":"WKSP47692",
"2753962522174656":"WKSP74656",
"7861826065021443":"WKSP21443",
"5818930449284907":"WKSP84907",
"3885245089615861":"WKSP15861",
"3252729122967870":"WKSP67870",
"72500119868823":"WKSP68823",
"8487760884965736":"WKSP65736",
"3846338071513068":"WKSP13068",
"4522271815743511":"WKSP43511",
"6583047541360945":"WKSP60945",
"2018181512448071":"WKSP48071",
"4855259627562093":"WKSP62093",
"2757335384646536":"WKSP46536",
"3392365184940059":"WKSP40059",
"2408339021443183":"WKSP43183",
"2801768232797684":"WKSP97684",
"2701504584282370":"WKSP82370",
"7696018671286272":"WKSP86272",
"1668791468712484":"WKSP12484",
"1630690252453020":"WKSP53020",
"5091995428464152":"WKSP64152",
"3426337764701105":"WKSP01105",
"6172435489215696":"WKSP15696",
"6595371237170435":"WKSP70435",
"4645731064325183":"WKSP25183",
"1137934926163478":"WKSP63478",
"7446409392065991":"WKSP65991",
"5399641329363812":"WKSP63812",
"3263552981129742":"WKSP29742",
"4419651421146654":"WKSP46654",
"334066153685537":"WKSP85537",
"2134120466078316":"WKSP78316",
"7539068082733522":"WKSP33522",
"8594751960923809":"WKSP23809",
"7645038045216930":"WKSP16930",
"472044062106526":"WKSP06526",
"3289343772031018":"WKSP31018",
"7672692013291290":"WKSP91290",
"4602047675588697":"WKSP88697",
"759764734828325":"WKSP28325",
"1659507799608045":"WKSP08045",
"1255689831172748":"WKSP72748",
"1466034059210723":"WKSP10723",
"7980003168560262":"WKSP60262",
"3041549555048763":"WKSP48763",
"3093429238214168":"WKSP14168",
"7024013628472719":"WKSP72719",
"2946735249443775":"WKSP43775",
"1246762801574507":"WKSP74507",
"5810129549763283":"WKSP63283",
"3212398972668523":"WKSP68523",
"1285710598227956":"WKSP27956",
"4763825283475044":"WKSP75044",
"2119777424002330":"WKSP02330",
"8691615572582525":"WKSP82525",
"7674599708099350":"WKSP99350",
"5255941993530599":"WKSP30599",
"2362124747904424":"WKSP04424",
"8429957623118669":"WKSP18669",
"2274515780923033":"WKSP23033",
"3857833776788560":"WKSP88560",
"3268197595939248":"WKSP39248",
"7445307272486361":"WKSP86361",
"5764722162401173":"WKSP01173",
"7532573037032010":"WKSP32010",
"417219079692790":"WKSP92790",
"6178934722919327":"WKSP19327",
"5183011280185325":"WKSP85325",
"4086310972964969":"WKSP64969",
"5370780280194938":"WKSP94938",
"6688808130562317":"WKSP62317",
"7569652049319615":"WKSP19615",
"6095948758255284":"WKSP55284",
"5207210401017252":"WKSP17252",
"6289934535338683":"WKSP38683",
"8968161481582697":"WKSP82697",
"3434051374059217":"WKSP59217",
"7116251338141142":"WKSP41142",
"3080316548359004":"WKSP59004",
"3888007492332761":"WKSP32761",
"1670828202955894":"WKSP55894",
"170306142227204":"WKSP27204",
"2526145285861888":"WKSP61888",
"7437306834008796":"WKSP08796",
"7654864394980066":"WKSP80066",
"8248242836415055":"WKSP15055",
"2441847280760502":"WKSP60502",
"848193321442892":"WKSP42892",
"5320794664216182":"WKSP16182",
"6509951041713500":"WKSP13500",
"3049863072168053":"WKSP68053",
"5206439413157315":"WKSP57315",
"919358551347097":"WKSP47097",
"3457621802558823":"WKSP58823",
"2408318793257193":"WKSP57193",
"1974343288999839":"WKSP99839",
"8658767343313594":"WKSP13594",
"2798126519344721":"WKSP44721",
"1516413757355523":"WKSP55523",
"4399840167193257":"WKSP93257",
"9000463045462805":"WKSP62805",
"5142803119924387":"WKSP24387",
"1841914579470365":"WKSP70365",
"5021560586871919":"WKSP71919",
"2850659059254224":"WKSP54224",
"4130781879827170":"WKSP27170",
"6554410713661746":"WKSP61746",
"1822897048441501":"WKSP41501",
"1608936977329653":"WKSP29653",
"1334492298648836":"WKSP48836",
"622609905561249":"WKSP61249",
"2233603208707980":"WKSP07980",
"5312939778437663":"WKSP37663",
"8951192101041839":"WKSP41839",
"3466026707913796":"WKSP13796",
"5062805680638439":"WKSP38439",
"2454212646575774":"WKSP75774",
"7319064609626129":"WKSP26129",
"1721787302637641":"WKSP37641",
"4686832614457882":"WKSP57882",
"2012364746847119":"WKSP47119",
"3027481791945860":"WKSP45860",
"8212819502932129":"WKSP32129",
"3901133651292642":"WKSP92642",
"7382186378478594":"WKSP78594",
"2588289406390116":"WKSP90116",
"3095165173071601":"WKSP71601",
"3087052809194108":"WKSP94108",
"3716587133923399":"WKSP23399",
"2519772061572153":"WKSP72153",
"3698685213882805":"WKSP82805",
"2595721780048287":"WKSP48287",
"1706675471431810":"WKSP31810",
"4009215006580163":"WKSP80163",
"6694152236561627":"WKSP61627",
"1778837453986721":"WKSP86721",
"4847272488917501":"WKSP17501",
"7520767347795747":"WKSP95747",
"7779871444488971":"WKSP88971",
"1700349563405171":"WKSP05171",
"3647309985879501":"WKSP79501",
"3390673279138160":"WKSP38160",
"5543397575946172":"WKSP46172",
"2378047012775422":"WKSP75422",
"5761979746353326":"WKSP53326",
"4484478780188185":"WKSP88185",
"3239614716757949":"WKSP57949",
"3350232408643587":"WKSP43587",
"428019227068099":"WKSP68099",
"6317139268808746":"WKSP08746",
"8427480748120":"WKSP48120",
"4441428932828057":"WKSP28057",
"1389491836282674":"WKSP82674",
"7263383353455795":"WKSP55795",
"7891994109736859":"WKSP36859",
"8747776687492813":"WKSP92813",
"8730323643658853":"WKSP58853",
"8209732072842473":"WKSP42473",
"4418713531220231":"WKSP20231",
"1763410951295333":"WKSP95333",
"55588287232512":"WKSP32512",
"5479376162331889":"WKSP31889",
"5361245525839690":"WKSP39690",
"3230238453479237":"WKSP79237",
"2432318169853804":"WKSP53804",
"1870976153635078":"WKSP35078",
"6554203424034102":"WKSP34102",
"4778505568535601":"WKSP35601",
"1088059735138778":"WKSP38778",
"4768514213375317":"WKSP75317",
"560313132195246":"WKSP95246",
"4539406526868800":"WKSP68800",
"8151547002510126":"WKSP10126",
"2591187269285912":"WKSP85912",
"251861698455680":"WKSP55680",
"1261766071535375":"WKSP35375",
"6702668125931340":"WKSP31340",
"2530306015204278":"WKSP04278",
"3866551603700884":"WKSP00884",
"1512055104543923":"WKSP43923",
"7845939183926620":"WKSP26620",
"6020741307388715":"WKSP88715",
"7491345728427874":"WKSP27874",
"4301376284584187":"WKSP84187",
"6935536957980197":"WKSP80197",
"8000110246518330":"WKSP18330",
"8417536419924417":"WKSP24417",
"6075122371477314":"WKSP77314",
"1209901303365609":"WKSP65609",
"8387878374294297":"WKSP94297",
"906398764319317":"WKSP19317",
"4191903690197760":"WKSP97760",
"3661611857596253":"WKSP96253",
"2219810816778143":"WKSP78143",
"5547775532559233":"WKSP59233",
"3577537535392767":"WKSP92767",
"4733014334286923":"WKSP86923",
"5414628259952744":"WKSP52744",
"2758458952985556":"WKSP85556",
"1397819168893144":"WKSP93144",
"1594847458178128":"WKSP78128",
"4675590300341545":"WKSP41545",
"2132568641746826":"WKSP46826",
"3011150083119087":"WKSP19087",
"4445627166611864":"WKSP11864",
"2045190809250849":"WKSP50849",
"5998983150097513":"WKSP97513",
"2690017451936431":"WKSP36431",
"7619278859209381":"WKSP09381",
"2951956042022684":"WKSP22684",
"5598129853451230":"WKSP51230",
"4044320188515418":"WKSP15418",
"6982468430370559":"WKSP70559",
"3764132255370756":"WKSP70756",
"7580217694832751":"WKSP32751",
"8105590045870301":"WKSP70301",
"1190654965107085":"WKSP07085",
"5208036183733024":"WKSP33024",
"294985579267985":"WKSP67985",
"355169633950204":"WKSP50204",
"6934889903045003":"WKSP45003",
"5691030177234968":"WKSP34968",
"6858644613911296":"WKSP11296",
"4093619043406708":"WKSP06708",
"4286901344022644":"WKSP22644",
"315629601460668":"WKSP60668",
"6532459568629659":"WKSP29659",
"3604275934515263":"WKSP15263",
"625126781029916":"WKSP29916",
"2798386718567310":"WKSP67310",
"5228295203918101":"WKSP18101",
"7590713518181508":"WKSP81508",
"79102086984519":"WKSP84519",
"2960670576095520":"WKSP95520",
"671716241771405":"WKSP71405",
"3512115360695701":"WKSP95701",
"4121142292770672":"WKSP70672",
"3303031703214835":"WKSP14835",
"6317054978828720":"WKSP28720",
"7405692867564439":"WKSP64439",
"5361657732059830":"WKSP59830",
"7140569922360600":"WKSP60600",
"3896106698418809":"WKSP18809",
"5498144830584322":"WKSP84322",
"2196118672704493":"WKSP04493",
"6313129442315904":"WKSP15904",
"772097134367206":"WKSP67206",
"984752964297111":"WKSP97111",
"8270923399252392":"WKSP52392",
"3586636320965362":"WKSP65362",
"6372470582661175":"WKSP61175",
"6737186107034535":"WKSP34535",
"318564432351549":"WKSP51549",
"2537364629049028":"WKSP49028",
"3677322739113657":"WKSP13657",
"3129619835435302":"WKSP35302",
"300076674159544":"WKSP59544",
"3939039009699715":"WKSP99715",
"5588921338997561":"WKSP97561",
"1894271342606041":"WKSP06041",
"1347839109641223":"FOO",
"6980265156470589":"WKSP70589",
"3172370213126200":"WKSP26200",
"6447127900489650":"WKSP89650",
"1835735182086440":"WKSP86440",
"3522882489084667":"WKSP84667",
"7139747037829954":"WKSP29954",
"4250931368076529":"WKSP76529",
"5260648398981548":"WKSP81548",
"6940774240735117":"WKSP35117",
"8445155504517186":"WKSP17186",
"5625395455411216":"WKSP11216",
"6225687580469845":"WKSP69845",
"1696964048181670":"WKSP81670",
"1294999171088847":"WKSP88847",
"8422494305718797":"WKSP18797",
"2060993628702366":"WKSP02366",
"4616383834712878":"WKSP12878",
"6064517193459159":"WKSP59159",
"3559503666355179":"WKSP55179",
"2566376260283895":"WKSP83895",
"74633484494038":"WKSP94038",
"534496186628022":"WKSP28022",
"6466196471043590":"WKSP43590",
"3088396029862490":"WKSP62490",
"7402783859712948":"WKSP12948",
"7351615570536896":"WKSP36896",
"5707594216405327":"WKSP05327",
"7125974409848534":"WKSP48534",
"5930167290418418":"WKSP18418",
"2419837044515056":"WKSP15056",
"3678399728027250":"WKSP27250",
"8934513401120327":"WKSP20327",
"3204972812151448":"WKSP51448",
"4546059186508554":"WKSP08554",
"2919876371702026":"WKSP02026",
"5208442328826162":"WKSP26162",
"3368060822412777":"WKSP12777",
"3565741877178786":"WKSP78786",
"1143871242911581":"WKSP11581",
"3565755707071390":"WKSP71390",
"3970213505518100":"WKSP18100",
"8513635177436830":"WKSP36830",
"5146095987044971":"WKSP44971",
"1343896547033594":"WKSP33594",
"2642990243112611":"WKSP12611",
"3011697725699826":"WKSP99826",
"3592758366865936":"WKSP65936",
"6148227880752714":"WKSP52714",
"939572893419636":"WKSP19636",
"3230445605593405":"WKSP93405",
"7280684959729003":"WKSP29003",
"6751253719992340":"WKSP92340",
"2882806483129615":"WKSP29615",
"2602675018942075":"WKSP42075",
"1480711142233587":"WKSP33587",
"8363157284278655":"WKSP78655",
"981192284730999":"WKSP30999",
"3693199425879370":"WKSP79370",
"857625898385740":"WKSP85740",
"4736855446736105":"WKSP36105",
"3266831174767158":"WKSP67158",
"5607228466448033":"WKSP48033",
"1450052712180829":"WKSP80829",
"8157169035022710":"WKSP22710",
"2223153850827124":"WKSP27124",
"1448987912621763":"WKSP21763",
"8037683426353983":"WKSP53983",
"5225262307320700":"WKSP20700",
"3595882606300628":"WKSP00628",
"3141306428453026":"WKSP53026",
"7050362484025379":"WKSP25379",
"7245993593792111":"WKSP92111",
"3342515786199776":"WKSP99776",
"5722547955842830":"WKSP42830",
"4684833476931954":"WKSP31954",
"4286606811544036":"WKSP44036",
"3915630010154769":"WKSP54769",
"1481452911205006":"WKSP05006",
"7049523663422216":"WKSP22216",
"2143445838866568":"WKSP66568",
"1200807089074241":"WKSP74241",
"5350499062434063":"WKSP34063",
"1733087917783065":"WKSP83065",
"2702160456597886":"WKSP97886",
"624783076386327":"WKSP86327",
"1374465857121694":"WKSP21694",
"8685033484765261":"WKSP65261",
"1945281551980788":"WKSP80788",
"2585180394242242":"WKSP42242",
"1546854956866710":"WKSP66710",
"1473623830442807":"WKSP42807",
"1207607018107195":"WKSP07195",
"7595510575976797":"WKSP76797",
"3995855360469211":"WKSP69211",
"3808821724597646":"WKSP97646",
"3309553696919402":"WKSP19402",
"1384765387752421":"WKSP52421",
"8881646887606982":"WKSP06982",
"6452891887772059":"WKSP72059",
"690280147998879":"WKSP98879",
"612624017082064":"WKSP82064",
"2314933093209006":"WKSP09006",
"1954792447280690":"WKSP80690",
"738820802727708":"WKSP27708",
"3607622508554759":"WKSP54759",
"2263691195002838":"WKSP02838",
"1260540844408131":"WKSP08131",
"7822803998989086":"WKSP89086",
"18163351930396":"WKSP30396",
"3523394195629188":"WKSP29188",
"3478481623936859":"WKSP36859",
"854449366718054":"WKSP18054",
"8538065613953888":"WKSP53888",
"2435820146101044":"WKSP01044",
"347699720749325":"WKSP49325",
"4115102977339205":"WKSP39205",
"207335237647565":"WKSP47565",
"3855600865649617":"WKSP49617",
"2728844378325734":"WKSP25734",
"476648923262593":"WKSP62593",
"2493732618258806":"WKSP58806",
"968639842805320":"WKSP05320",
"4335793765270774":"WKSP70774",
"1369835350734933":"WKSP34933",
"3307242972776258":"WKSP76258",
"4420086038348026":"WKSP48026",
"2243091828969641":"WKSP69641",
"1847812431050861":"WKSP50861",
"155715089583000":"WKSP83000",
"420409780826878":"WKSP26878",
"522548490841242":"WKSP41242",
"7683320177840435":"WKSP40435",
"5357987331498183":"WKSP98183",
"3608061076499792":"WKSP99792",
"4316236923499490":"WKSP99490",
"3489481192353978":"WKSP53978",
"3074747072454994":"WKSP54994",
"934654992300950":"WKSP00950",
"2268785406081160":"WKSP81160",
"2935749633015612":"WKSP15612",
"1670430571006352":"WKSP06352",
"4294669027849375":"WKSP49375",
"3350560415939696":"WKSP39696",
"2046372413950853":"WKSP50853",
"861272053797269":"WKSP97269",
"4265602985510465":"WKSP10465",
"2604442385442803":"WKSP42803",
"4305843110625780":"WKSP25780",
"1244372366638307":"WKSP38307",
"1066222518988099":"WKSP88099",
"4097180419831755":"WKSP31755",
"755498123027169":"WKSP27169",
"4042987526980271":"WKSP80271",
"409660386782661":"WKSP82661",
"2995946909074314":"WKSP74314",
"3916048213604727":"WKSP04727",
"41251265890198":"WKSP90198",
"4564233832315804":"WKSP15804",
"628480321109475":"WKSP09475",
"4014729437266818":"WKSP66818",
"3242201110291567":"WKSP91567",
"3403850854017610":"WKSP17610",
"2048338924564521":"WKSP64521",
"3545125026937454":"WKSP37454",
"2928367091313000":"WKSP13000",
"4788804191210767":"WKSP10767",
"7089546184589298":"WKSP89298",
"2583945689673647":"WKSP73647",
"3025562534866537":"WKSP66537",
"1271172149757486":"WKSP57486",
"5164300075551723":"WKSP51723",
"2481510715676972":"WKSP76972",
"8353382718778393":"WKSP78393",
"2918901385251377":"WKSP51377",
"8867907803206451":"WKSP06451",
"6341843114583650":"WKSP83650",
"916085649695346":"WKSP95346",
"3481680945278256":"WKSP78256",
"1066984229201526":"WKSP01526",
"8365423149063440":"WKSP63440",
"8301283563205791":"WKSP05791",
"5594018612472467":"WKSP72467",
"6627977569747230":"WKSP47230",
"4301772489432883":"WKSP32883",
"7543168827092757":"WKSP92757",
"6670353624678057":"WKSP78057",
"4323528493510570":"WKSP10570",
"7824073848930194":"WKSP30194",
"594335339241995":"WKSP41995",
"1130149126681780":"WKSP81780",
"1162223255123301":"WKSP23301",
"4367439869409308":"WKSP09308",
"5315474996076751":"WKSP76751",
"2049608036756863":"WKSP56863",
"1630230512406485":"WKSP06485",
"8396760938032827":"WKSP32827",
"7818379050140290":"WKSP40290",
"264952617920968":"WKSP20968",
"8204380433552505":"WKSP52505",
"8768951399561091":"WKSP61091",
"4769419618016788":"WKSP16788",
"6548358018039961":"WKSP39961",
"6961980370448706":"WKSP48706",
"7591280735606570":"WKSP06570",
"8620611698756429":"WKSP56429",
"8028400393858643":"WKSP58643",
"956170717353795":"WKSP53795",
"2302655566636834":"WKSP36834",
"1724001792755754":"WKSP55754",
"2636660604303869":"WKSP03869",
"993220210098260":"WKSP98260",
"3642124255376493":"WKSP76493",
"5882486947629410":"WKSP29410",
"8612050343461662":"WKSP61662",
"1486067088316562":"WKSP16562",
"5721705952431138":"WKSP31138",
"2800185651060322":"WKSP60322",
"5044588756756512":"WKSP56512",
"46091267665337":"WKSP65337",
"7571925036963148":"WKSP63148",
"2573194016123929":"WKSP23929",
"5994489853600084":"WKSP00084",
"3388148585318986":"WKSP18986",
"2005970059961820":"WKSP61820",
"1416596581703225":"WKSP03225",
"7853821278396848":"WKSP96848",
"8849465689008419":"WKSP08419",
"4482346837849532":"WKSP49532",
"7880475610338708":"WKSP38708",
"7918187416399190":"WKSP99190",
"5773731387265464":"WKSP65464",
"6093419230075293":"WKSP75293",
"4899089966314969":"WKSP14969",
"7288084661647242":"WKSP47242",
"7246000333016409":"WKSP16409",
"8057355011312750":"WKSP12750",
"6796816327974220":"WKSP74220",
"3919702029111270":"WKSP11270",
"5550530936374202":"WKSP74202",
"4104328240019648":"WKSP19648",
"5189490987493033":"WKSP93033",
"3618863407103266":"WKSP03266",
"6930261486336685":"WKSP36685",
"6283425183421325":"WKSP21325",
"6189288598693423":"WKSP93423",
"8242395166098133":"WKSP98133",
"8081402048659131":"WKSP59131",
"6903919095079587":"WKSP79587",
"392802645302769":"WKSP02769",
"8209011193698505":"WKSP98505",
"3992774525425257":"WKSP25257",
"8699566513738185":"WKSP38185",
"4675523852932415":"WKSP32415",
"5892437377853602":"WKSP53602",
"1644452145602079":"WKSP02079",
"6731601019007878":"WKSP07878",
"5994571398884932":"WKSP84932",
"641283800741766":"WKSP41766",
"6674302943445269":"WKSP45269",
"4512030377518744":"WKSP18744",
"2861846839590480":"WKSP90480",
"7753229903624385":"WKSP24385",
"7008514674996233":"WKSP96233",
"7577644282420798":"WKSP20798",
"4543798963419228":"WKSP19228",
"1215832677402510":"WKSP02510",
"7486474528420178":"WKSP20178",
"4874140476606511":"WKSP06511",
"8405830731420074":"WKSP20074",
"1430042391883921":"WKSP83921",
"1190319230103612":"WKSP03612",
"2534203775956993":"WKSP56993",
"6241902221856131":"WKSP56131",
"1552722138559212":"WKSP59212",
"4021013784719324":"WKSP19324",
"8342003283415856":"WKSP15856",
"2512012596533114":"WKSP33114",
"2718826291907906":"WKSP07906",
"4243275257108077":"WKSP08077",
"3816082891959":"WKSP91959",
"6694020275033256":"WKSP33256",
"1024569334471304":"WKSP71304",
"1996078668426696":"WKSP26696",
"8462771638739534":"WKSP39534",
"8373270785405870":"WKSP05870",
"2638012953964866":"WKSP64866",
"2030204539241222":"WKSP41222",
"5980810869200702":"WKSP00702",
"8090678092407623":"WKSP07623",
"4476435072513247":"WKSP13247",
"3604893583724339":"WKSP24339",
"4078303680968427":"WKSP68427",
"8869402718018389":"WKSP18389",
"7757351541270797":"WKSP70797",
"6839459401781755":"WKSP81755",
"5812332634286707":"WKSP86707",
"1075135489709736":"WKSP09736",
"8919645323426113":"WKSP26113",
"915487813497861":"WKSP97861",
"2297137194508466":"WKSP08466",
"6160344302447052":"WKSP47052",
"4059328729268872":"WKSP68872",
"7427496141910820":"WKSP10820",
"7785587028305613":"WKSP05613",
"5342488668334761":"WKSP34761",
"4631636298166861":"WKSP66861",
"2727033710157880":"WKSP57880",
"496095856129906":"WKSP29906",
"643920258025167":"WKSP25167",
"23252064048803":"WKSP48803",
"6470248700575495":"WKSP75495",
"4258007360051163":"WKSP51163",
"5468803723918345":"WKSP18345",
"6513750945892161":"WKSP92161",
"8116419452726956":"WKSP26956",
"4348167575124846":"WKSP24846",
"2225653022210797":"WKSP10797",
"583871904400745":"WKSP00745",
"6170170487126515":"WKSP26515",
"7720974219365212":"WKSP65212",
"1226420522062922":"WKSP62922",
"198078467972670":"WKSP72670",
"7421122906265058":"WKSP65058",
"7773614452909365":"WKSP09365",
"2007758040337044":"WKSP37044",
"3059735186595105":"WKSP95105",
"919181141757393":"WKSP57393",
"5571068294043718":"WKSP43718",
"1401058008731807":"WKSP31807",
"7183569555357024":"WKSP57024",
"5059940275087360":"WKSP87360",
"2468096614810406":"WKSP10406",
"8402517018838192":"WKSP38192",
"4707166292052266":"WKSP52266",
"2131093245568906":"WKSP68906",
"8596220602934265":"WKSP34265",
"4862223949650295":"WKSP50295",
"5039403346765951":"WKSP65951",
"50874099931053":"WKSP31053",
"5637444917083286":"WKSP83286",
"8580098605378900":"WKSP78900",
"4632053605658134":"WKSP58134",
"8234879311726229":"WKSP26229",
"382266861287659":"WKSP87659",
"1259846182900316":"WKSP00316",
"1655394085364849":"WKSP64849",
"1461414759578709":"WKSP78709",
"8762929547267412":"WKSP67412",
"4553905186529436":"WKSP29436",
"6287301359266579":"WKSP66579",
"4683391640040887":"WKSP40887",
"8443804034039729":"WKSP39729",
"2671887848760902":"WKSP60902",
"1797714960873665":"WKSP73665",
"2174796119045767":"WKSP45767",
"1788883550750333":"WKSP50333",
"4216778272420997":"WKSP20997",
"6978381486637006":"WKSP37006",
"7302813575846642":"WKSP46642",
"4256016512840835":"WKSP40835",
"6950966552122777":"WKSP22777",
"2155190703588151":"WKSP88151",
"7062171663452845":"WKSP52845",
"1966003873193471":"WKSP93471",
"5919435252237255":"WKSP37255",
"8030885408235938":"WKSP35938",
"5936570518305495":"WKSP05495",
"2048370243392411":"WKSP92411",
"4069914813511432":"WKSP11432",
"1296808950191768":"WKSP91768",
"8526491788464115":"WKSP64115",
"4812737453039226":"WKSP39226",
"8989028150242666":"WKSP42666",
"8501683575365818":"WKSP65818",
"5961148623145640":"WKSP45640",
"7547932970365792":"WKSP65792",
"280728425411849":"WKSP11849",
"8415791334163083":"WKSP63083",
"216980570999213":"WKSP99213",
"3502607489602879":"WKSP02879",
"2180678652224651":"WKSP24651",
"7044756467324477":"WKSP24477",
"1167056430318609":"WKSP18609",
"5255049652433387":"WKSP33387",
"8033951375507893":"WKSP07893",
"6513264624445277":"WKSP45277",
"6756174938473754":"WKSP73754",
"6352448007590494":"WKSP90494",
"4175782211451487":"WKSP51487",
"3554939056964806":"WKSP64806",
"2953547188421304":"WKSP21304",
"1911501886401872":"WKSP01872",
"4587307470782452":"WKSP82452",
"2662457344742181":"WKSP42181",
"3589698936511827":"WKSP11827",
"499080101033657":"WKSP33657",
"4750844257440911":"WKSP40911",
"6023967872560555":"WKSP60555",
"2211971500252427":"WKSP52427",
"2787869455630078":"WKSP30078",
"5205279068054683":"WKSP54683",
"8094636550028227":"WKSP28227",
"7467014282339411":"WKSP39411",
"4558793219414507":"WKSP14507",
"5567440357408726":"WKSP08726",
"1028089269467297":"WKSP67297",
"8651938853345477":"WKSP45477",
"7928911561030569":"WKSP30569",
"3159172995827621":"WKSP27621",
"3193886606863033":"WKSP63033",
"7272508181837468":"WKSP37468",
"6233146000186311":"WKSP86311",
"4306447279222804":"WKSP22804",
"4020876629989403":"WKSP89403",
"5851794625717020":"WKSP17020",
"8384927649929152":"WKSP29152",
"3006939330184357":"WKSP84357",
"368653989522120":"WKSP22120",
"7579871474827869":"WKSP27869",
"2879807727336976":"WKSP36976",
"8067720013726996":"WKSP26996",
"8721720268094374":"WKSP94374",
"5345632176437689":"WKSP37689",
"6651237597690165":"WKSP90165",
"2081835548057656":"WKSP57656",
"2564081902482101":"WKSP82101",
"1250761573926989":"WKSP26989",
"4374487565911596":"WKSP11596",
"8632039725583455":"WKSP83455",
"3925370244508409":"WKSP08409",
"3691773970315261":"WKSP15261",
"803149071262205":"WKSP62205",
"2109773271383999":"WKSP83999",
"3914954915086692":"WKSP86692",
"1625436338790932":"WKSP90932",
"6544328396002769":"WKSP02769",
"3429386033568931":"WKSP68931",
"3922231503949947":"WKSP49947",
"6683853670522680":"WKSP22680",
"731236904994184":"WKSP94184",
"7812720367780203":"WKSP80203",
"1147527465045056":"WKSP45056",
"3113800634915098":"WKSP15098",
"11849391330586":"WKSP30586",
"8118786433116950":"WKSP16950",
"5772278922433845":"WKSP33845",
"1993960642867532":"WKSP67532",
"2535085412176404":"WKSP76404",
"4134139000279565":"WKSP79565",
"3041426017561321":"WKSP61321",
"8406687072355136":"WKSP55136",
"1848610337132241":"WKSP32241"
}


# COMMAND ----------

# DBTITLE 1,System tables


SYSTEM_TABLES_INCREMENT={
	"system.access.audit":"event_time",
	"system.access.column_lineage":"event_time",
	"system.access.table_lineage":"event_time",
	"system.billing.usage":"usage_start_time",
	"system.compute.node_timeline":"start_time",
	"system.compute.warehouse_events":"event_time"
# ,
#	"system.marketplace.listing_access_events":"event_time",
#	"system.marketplace.listing_funnel_events":"event_time",
#	"system.query.history":"start_time",
#	"system.storage.predictive_optimization_operations_history":"start_time"
}


SYSTEM_TABLES_REPLACE=[
	"system.billing.list_prices",
	"system.compute.clusters",
	"system.compute.node_types"
]




# COMMAND ----------

# MAGIC %md
# MAGIC Unlikely to change parameters below

# COMMAND ----------

# DBTITLE 1,Behavior parameters
MAX_RESULTS_PER_PAGE = 1000
# TODO: Make thos larger
MAX_PAGES_PER_RUN = 1000
PAGE_SIZE = 250 # 250 is the max
# Will force merge on ID rather than append only
FORCE_MERGE_INCREMENTAL = False

# COMMAND ----------

# DBTITLE 1,API Config
# Please ensure the url starts with https and DOES NOT have a slash at the end

WAREHOUSES_URL = "{0}/api/2.0/sql/warehouses".format(WORKSPACE_HOST) ## SQL Warehouses APIs 2.1
# No time parameter option
JOBS_URL = "{0}/api/2.1/jobs/list".format(WORKSPACE_HOST) ## Jobs & Workflows History API 2.1 
# No time parameter option
CLUSTERS_URL = "{0}/api/2.0/clusters/list".format(WORKSPACE_HOST)
# No time parameter option
DASHBOARDS_URL = "{0}/api/2.0/preview/sql/dashboards".format(WORKSPACE_HOST) ## Queries and Dashboards API - ❗️in preview, deprecated soon❗️
# No time parameter option
WORKSPACE_OBJECTS_URL = "{0}/api/2.0/workspace/list".format(WORKSPACE_HOST)
# notebooks_modified_after integer
# UTC timestamp in milliseconds
INSTANCE_POOLS_URL = "{0}/api/2.0/instance-pools/list".format(WORKSPACE_HOST)
# No time parameter option
DLT_PIPELINES_URL = "{0}/api/2.0/pipelines".format(WORKSPACE_HOST)
# No time parameter option
# TODO: download pipeline events..
# /api/2.0/pipelines/{pipeline_id}/events
JOB_RUNS_URL = "{0}/api/2.1/jobs/runs/list".format(WORKSPACE_HOST) 
# start_time_from int64
# Show runs that started at or after this value. The value must be a UTC timestamp in milliseconds.  
# start_time_to int64
# Show runs that started at or before this value. The value must be a UTC timestamp in milliseconds. 



# COMMAND ----------

# DBTITLE 1,Database and Table Config

WAREHOUSES_TABLE_NAME = SCHEMA_NAME+".warehouses"
JOBS_TABLE_NAME = SCHEMA_NAME+".jobs"
DASHBOARDS_TABLE_NAME = SCHEMA_NAME+".dashboards_preview"
CLUSTERS_TABLE_NAME = SCHEMA_NAME+".clusters_pinned"
WORKSPACE_OBJECTS_TABLE_NAME = SCHEMA_NAME+".workspace_objects"
INSTANCE_POOLS_TABLE_NAME = SCHEMA_NAME+".instance_pools"
DLT_PIPELINES_TABLE_NAME = SCHEMA_NAME+".dlt_pipelines"
JOB_RUNS_TABLE_NAME = SCHEMA_NAME+".job_runs"
DBU_DISCOUNT_TABLE_NAME = SCHEMA_NAME+".dbu_discounts"
INFRA_MARKUP_TABLE_NAME = SCHEMA_NAME+".infra_markups"
WORKSPACE_TABLE_NAME = SCHEMA_NAME+".workspaces"
SYSTEM_TABLE_SCHEMA_PREFIX = "system_"
