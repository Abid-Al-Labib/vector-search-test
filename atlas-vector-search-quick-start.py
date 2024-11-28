import pymongo
import datetime

# connect to your Atlas cluster
client = pymongo.MongoClient("mongodb+srv://techabidallabib:L9QzyLDpkZYzdWDI@vectorclustertest.pxmtf.mongodb.net/?retryWrites=true&w=majority&appName=vectorClusterTest")

# define pipeline
pipeline = [
  {
    '$vectorSearch': {
      'index': 'vector_index', 
      'path': 'plot_embedding', 
      'queryVector': [-0.0016261312, -0.028070757, -0.011342932, -0.012775794, -0.0027440966, 0.008683807, -0.02575152, -0.02020668, -0.010283281, -0.0041719596, 0.021392956, 0.028657231, -0.006634482, 0.007490867, 0.018593878, 0.0038187427, 0.029590257, -0.01451522, 0.016061379, 0.00008528442, -0.008943722, 0.01627464, 0.024311995, -0.025911469, 0.00022596726, -0.008863748, 0.008823762, -0.034921836, 0.007910728, -0.01515501, 0.035801545, -0.0035688248, -0.020299982, -0.03145631, -0.032256044, -0.028763862, -0.0071576433, -0.012769129, 0.012322609, -0.006621153, 0.010583182, 0.024085402, -0.001623632, 0.007864078, -0.021406285, 0.002554159, 0.012229307, -0.011762793, 0.0051682983, 0.0048484034, 0.018087378, 0.024325324, -0.037694257, -0.026537929, -0.008803768, -0.017767483, -0.012642504, -0.0062712682, 0.0009771782, -0.010409906, 0.017754154, -0.004671795, -0.030469967, 0.008477209, -0.005218282, -0.0058480743, -0.020153364, -0.0032805866, 0.004248601, 0.0051449724, 0.006791097, 0.007650814, 0.003458861, -0.0031223053, -0.01932697, -0.033615597, 0.00745088, 0.006321252, -0.0038154104, 0.014555207, 0.027697546, -0.02828402, 0.0066711367, 0.0077107945, 0.01794076, 0.011349596, -0.0052715978, 0.014755142, -0.019753495, -0.011156326, 0.011202978, 0.022126047, 0.00846388, 0.030549942, -0.0041386373, 0.018847128, -0.00033655585, 0.024925126, -0.003555496, -0.019300312, 0.010749794, 0.0075308536, -0.018287312, -0.016567878, -0.012869096, -0.015528221, 0.0078107617, -0.011156326, 0.013522214, -0.020646535, -0.01211601, 0.055928253, 0.011596181, -0.017247654, 0.0005939711, -0.026977783, -0.003942035, -0.009583511, -0.0055248477, -0.028737204, 0.023179034, 0.003995351, 0.0219661, -0.008470545, 0.023392297, 0.010469886, -0.015874773, 0.007890735, -0.009690142, -0.00024970944, 0.012775794, 0.0114762215, 0.013422247, 0.010429899, -0.03686786, -0.006717788, -0.027484283, 0.011556195, -0.036068123, -0.013915418, -0.0016327957, 0.0151016945, -0.020473259, 0.004671795, -0.012555866, 0.0209531, 0.01982014, 0.024485271, 0.0105431955, -0.005178295, 0.033162415, -0.013795458, 0.007150979, 0.010243294, 0.005644808, 0.017260984, -0.0045618312, 0.0024725192, 0.004305249, -0.008197301, 0.0014203656, 0.0018460588, 0.005015015, -0.011142998, 0.01439526, 0.022965772, 0.02552493, 0.007757446, -0.0019726837, 0.009503538, -0.032042783, 0.008403899, -0.04609149, 0.013808787, 0.011749465, 0.036388017, 0.016314628, 0.021939443, -0.0250051, -0.017354285, -0.012962398, 0.00006107364, 0.019113706, 0.03081652, -0.018114036, -0.0084572155, 0.009643491, -0.0034721901, 0.0072642746, -0.0090636825, 0.01642126, 0.013428912, 0.027724205, 0.0071243206, -0.6858542, -0.031029783, -0.014595194, -0.011449563, 0.017514233, 0.01743426, 0.009950057, 0.0029706885, -0.015714826, -0.001806072, 0.011856096, 0.026444625, -0.0010663156, -0.006474535, 0.0016161345, -0.020313311, 0.0148351155, -0.0018393943, 0.0057347785, 0.018300641, -0.018647194, 0.03345565, -0.008070676, 0.0071443142, 0.014301958, 0.0044818576, 0.003838736, -0.007350913, -0.024525259, -0.001142124, -0.018620536, 0.017247654, 0.007037683, 0.010236629, 0.06046009, 0.0138887605, -0.012122675, 0.037694257, 0.0055081863, 0.042492677, 0.00021784494, -0.011656162, 0.010276617, 0.022325981, 0.005984696, -0.009496873, 0.013382261, -0.0010563189, 0.0026507939, -0.041639622, 0.008637156, 0.026471283, -0.008403899, 0.024858482, -0.00066686375, -0.0016252982, 0.027590916, 0.0051449724, 0.0058647357, -0.008743787, -0.014968405, 0.027724205, -0.011596181, 0.0047650975, -0.015381602, 0.0043718936, 0.002159289, 0.035908177, -0.008243952, -0.030443309, 0.027564257, 0.042625964, -0.0033688906, 0.01843393, 0.019087048, 0.024578573, 0.03268257, -0.015608194, -0.014128681, -0.0033538956, -0.0028757197, -0.004121976, -0.032389335, 0.0034322033, 0.058807302, 0.010943064, -0.030523283, 0.008903735, 0.017500903, 0.00871713, -0.0029406983, 0.013995391, -0.03132302, -0.019660193, -0.00770413, -0.0038853872, 0.0015894766, -0.0015294964, -0.006251275, -0.021099718, -0.010256623, -0.008863748, 0.028550599, 0.02020668, -0.0012962399, -0.003415542, -0.0022509254, 0.0119360695, 0.027590916, -0.046971202, -0.0015194997, -0.022405956, 0.0016677842, -0.00018535563, -0.015421589, -0.031802863, 0.03814744, 0.0065411795, 0.016567878, -0.015621523, 0.022899127, -0.011076353, 0.02841731, -0.002679118, -0.002342562, 0.015341615, 0.01804739, -0.020566562, -0.012989056, -0.002990682, 0.01643459, 0.00042527664, 0.008243952, -0.013715484, -0.004835075, -0.009803439, 0.03129636, -0.021432944, 0.0012087687, -0.015741484, -0.0052016205, 0.00080890034, -0.01755422, 0.004811749, -0.017967418, -0.026684547, -0.014128681, 0.0041386373, -0.013742141, -0.010056688, -0.013268964, -0.0110630235, -0.028337335, 0.015981404, -0.00997005, -0.02424535, -0.013968734, -0.028310679, -0.027750863, -0.020699851, 0.02235264, 0.001057985, 0.00081639783, -0.0099367285, 0.013522214, -0.012016043, -0.00086471526, 0.013568865, 0.0019376953, -0.019020405, 0.017460918, -0.023045745, 0.008503866, 0.0064678704, -0.011509543, 0.018727167, -0.003372223, -0.0028690554, -0.0027024434, -0.011902748, -0.012182655, -0.015714826, -0.0098634185, 0.00593138, 0.018753825, 0.0010146659, 0.013029044, 0.0003521757, -0.017620865, 0.04102649, 0.00552818, 0.024485271, -0.009630162, -0.015608194, 0.0006718621, -0.0008418062, 0.012395918, 0.0057980907, 0.016221326, 0.010616505, 0.004838407, -0.012402583, 0.019900113, -0.0034521967, 0.000247002, -0.03153628, 0.0011038032, -0.020819811, 0.016234655, -0.00330058, -0.0032289368, 0.00078973995, -0.021952773, -0.022459272, 0.03118973, 0.03673457, -0.021472929, 0.0072109587, -0.015075036, 0.004855068, -0.0008151483, 0.0069643734, 0.010023367, -0.010276617, -0.023019087, 0.0068244194, -0.0012520878, -0.0015086699, 0.022046074, -0.034148756, -0.0022192693, 0.002427534, -0.0027124402, 0.0060346797, 0.015461575, 0.0137554705, 0.009230294, -0.009583511, 0.032629255, 0.015994733, -0.019167023, -0.009203636, 0.03393549, -0.017274313, -0.012042701, -0.0009930064, 0.026777849, -0.013582194, -0.0027590916, -0.017594207, -0.026804507, -0.0014236979, -0.022032745, 0.0091236625, -0.0042419364, -0.00858384, -0.0033905501, -0.020739838, 0.016821127, 0.022539245, 0.015381602, 0.015141681, 0.028817179, -0.019726837, -0.0051283115, -0.011489551, -0.013208984, -0.0047017853, -0.0072309524, 0.01767418, 0.0025658219, -0.010323267, 0.012609182, -0.028097415, 0.026871152, -0.010276617, 0.021912785, 0.0022542577, 0.005124979, -0.0019710176, 0.004518512, -0.040360045, 0.010969722, -0.0031539614, -0.020366628, -0.025778178, -0.0110030435, -0.016221326, 0.0036587953, 0.016207997, 0.003007343, -0.0032555948, 0.0044052163, -0.022046074, -0.0008822095, -0.009363583, 0.028230704, -0.024538586, 0.0029840174, 0.0016044717, -0.014181997, 0.031349678, -0.014381931, -0.027750863, 0.02613806, 0.0004136138, -0.005748107, -0.01868718, -0.0010138329, 0.0054348772, 0.010703143, -0.003682121, 0.0030856507, -0.004275259, -0.010403241, 0.021113047, -0.022685863, -0.023032416, 0.031429652, 0.001792743, -0.005644808, -0.011842767, -0.04078657, -0.0026874484, 0.06915057, -0.00056939584, -0.013995391, 0.010703143, -0.013728813, -0.022939114, -0.015261642, -0.022485929, 0.016807798, 0.007964044, 0.0144219175, 0.016821127, 0.0076241563, 0.005461535, -0.013248971, 0.015301628, 0.0085171955, -0.004318578, 0.011136333, -0.0059047225, -0.010249958, -0.018207338, 0.024645219, 0.021752838, 0.0007614159, -0.013648839, 0.01111634, -0.010503208, -0.0038487327, -0.008203966, -0.00397869, 0.0029740208, 0.008530525, 0.005261601, 0.01642126, -0.0038753906, -0.013222313, 0.026537929, 0.024671877, -0.043505676, 0.014195326, 0.024778508, 0.0056914594, -0.025951454, 0.017620865, -0.0021359634, 0.008643821, 0.021299653, 0.0041686273, -0.009017031, 0.04044002, 0.024378639, -0.027777521, -0.014208655, 0.0028623908, 0.042119466, 0.005801423, -0.028124074, -0.03129636, 0.022139376, -0.022179363, -0.04067994, 0.013688826, 0.013328944, 0.0046184794, -0.02828402, -0.0063412455, -0.0046184794, -0.011756129, -0.010383247, -0.0018543894, -0.0018593877, -0.00052024535, 0.004815081, 0.014781799, 0.018007403, 0.01306903, -0.020433271, 0.009043689, 0.033189073, -0.006844413, -0.019766824, -0.018767154, 0.00533491, -0.0024575242, 0.018727167, 0.0058080875, -0.013835444, 0.0040719924, 0.004881726, 0.012029372, 0.005664801, 0.03193615, 0.0058047553, 0.002695779, 0.009290274, 0.02361889, 0.017834127, 0.0049017193, -0.0036388019, 0.010776452, -0.019793482, 0.0067777685, -0.014208655, -0.024911797, 0.002385881, 0.0034988478, 0.020899786, -0.0025858153, -0.011849431, 0.033189073, -0.021312982, 0.024965113, -0.014635181, 0.014048708, -0.0035921505, -0.003347231, 0.030869836, -0.0017161017, -0.0061346465, 0.009203636, -0.025165047, 0.0068510775, 0.021499587, 0.013782129, -0.0024475274, -0.0051149824, -0.024445284, 0.006167969, 0.0068844, -0.00076183246, 0.030150073, -0.0055948244, -0.011162991, -0.02057989, -0.009703471, -0.020646535, 0.008004031, 0.0066378145, -0.019900113, -0.012169327, -0.01439526, 0.0044252095, -0.004018677, 0.014621852, -0.025085073, -0.013715484, -0.017980747, 0.0071043274, 0.011456228, -0.01010334, -0.0035321703, -0.03801415, -0.012036037, -0.0028990454, -0.05419549, -0.024058744, -0.024272008, 0.015221654, 0.027964126, 0.03182952, -0.015354944, 0.004855068, 0.011522872, 0.004771762, 0.0027874154, 0.023405626, 0.0004242353, -0.03132302, 0.007057676, 0.008763781, -0.0027057757, 0.023005757, -0.0071176565, -0.005238275, 0.029110415, -0.010989714, 0.013728813, -0.009630162, -0.029137073, -0.0049317093, -0.0008630492, -0.015248313, 0.0043219104, -0.0055681667, -0.013175662, 0.029723546, 0.025098402, 0.012849103, -0.0009996708, 0.03118973, -0.0021709518, 0.0260181, -0.020526575, 0.028097415, -0.016141351, 0.010509873, -0.022965772, 0.002865723, 0.0020493253, 0.0020509914, -0.0041419696, -0.00039695262, 0.017287642, 0.0038987163, 0.014795128, -0.014661839, -0.008950386, 0.004431874, -0.009383577, 0.0012604183, -0.023019087, 0.0029273694, -0.033135757, 0.009176978, -0.011023037, -0.002102641, 0.02663123, -0.03849399, -0.0044152127, 0.0004527676, -0.0026924468, 0.02828402, 0.017727496, 0.035135098, 0.02728435, -0.005348239, -0.001467017, -0.019766824, 0.014715155, 0.011982721, 0.0045651635, 0.023458943, -0.0010046692, -0.0031373003, -0.0006972704, 0.0019043729, -0.018967088, -0.024311995, 0.0011546199, 0.007977373, -0.004755101, -0.010016702, -0.02780418, -0.004688456, 0.013022379, -0.005484861, 0.0017227661, -0.015394931, -0.028763862, -0.026684547, 0.0030589928, -0.018513903, 0.028363993, 0.0044818576, -0.009270281, 0.038920518, -0.016008062, 0.0093902415, 0.004815081, -0.021059733, 0.01451522, -0.0051583014, 0.023765508, -0.017874114, -0.016821127, -0.012522544, -0.0028390652, 0.0040886537, 0.020259995, -0.031216389, -0.014115352, -0.009176978, 0.010303274, 0.020313311, 0.0064112223, -0.02235264, -0.022872468, 0.0052449396, 0.0005723116, 0.0037321046, 0.016807798, -0.018527232, -0.009303603, 0.0024858483, -0.0012662497, -0.007110992, 0.011976057, -0.007790768, -0.042999174, -0.006727785, -0.011829439, 0.007024354, 0.005278262, -0.017740825, -0.0041519664, 0.0085905045, 0.027750863, -0.038387362, 0.024391968, 0.00087721116, 0.010509873, -0.00038508154, -0.006857742, 0.0183273, -0.0037054466, 0.015461575, 0.0017394272, -0.0017944091, 0.014181997, -0.0052682655, 0.009023695, 0.00719763, -0.013522214, 0.0034422, 0.014941746, -0.0016711164, -0.025298337, -0.017634194, 0.0058714002, -0.005321581, 0.017834127, 0.0110630235, -0.03369557, 0.029190388, -0.008943722, 0.009363583, -0.0034222065, -0.026111402, -0.007037683, -0.006561173, 0.02473852, -0.007084334, -0.010110005, -0.008577175, 0.0030439978, -0.022712521, 0.0054582027, -0.0012620845, -0.0011954397, -0.015741484, 0.0129557345, -0.00042111133, 0.00846388, 0.008930393, 0.016487904, 0.010469886, -0.007917393, -0.011762793, -0.0214596, 0.000917198, 0.021672864, 0.010269952, -0.007737452, -0.010243294, -0.0067244526, -0.015488233, -0.021552904, 0.017127695, 0.011109675, 0.038067464, 0.00871713, -0.0025591573, 0.021312982, -0.006237946, 0.034628596, -0.0045251767, 0.008357248, 0.020686522, 0.0010696478, 0.0076708077, 0.03772091, -0.018700508, -0.0020676525, -0.008923728, -0.023298996, 0.018233996, -0.010256623, 0.0017860786, 0.009796774, -0.00897038, -0.01269582, -0.018527232, 0.009190307, -0.02372552, -0.042119466, 0.008097334, -0.0066778013, -0.021046404, 0.0019593548, 0.011083017, -0.0016028056, 0.012662497, -0.000059095124, 0.0071043274, -0.014675168, 0.024831824, -0.053582355, 0.038387362, 0.0005698124, 0.015954746, 0.021552904, 0.031589597, -0.009230294, -0.0006147976, 0.002625802, -0.011749465, -0.034362018, -0.0067844326, -0.018793812, 0.011442899, -0.008743787, 0.017474247, -0.021619547, 0.01831397, -0.009037024, -0.0057247817, -0.02728435, 0.010363255, 0.034415334, -0.024032086, -0.0020126705, -0.0045518344, -0.019353628, -0.018340627, -0.03129636, -0.0034038792, -0.006321252, -0.0016161345, 0.033642255, -0.000056075285, -0.005005019, 0.004571828, -0.0024075406, -0.00010215386, 0.0098634185, 0.1980148, -0.003825407, -0.025191706, 0.035161756, 0.005358236, 0.025111731, 0.023485601, 0.0023342315, -0.011882754, 0.018287312, -0.0068910643, 0.003912045, 0.009243623, -0.001355387, -0.028603915, -0.012802451, -0.030150073, -0.014795128, -0.028630573, -0.0013487226, 0.002667455, 0.00985009, -0.0033972147, -0.021486258, 0.009503538, -0.017847456, 0.013062365, -0.014341944, 0.005078328, 0.025165047, -0.015594865, -0.025924796, -0.0018177348, 0.010996379, -0.02993681, 0.007324255, 0.014475234, -0.028577257, 0.005494857, 0.00011725306, -0.013315615, 0.015941417, 0.009376912, 0.0025158382, 0.008743787, 0.023832154, -0.008084005, -0.014195326, -0.008823762, 0.0033455652, -0.032362677, -0.021552904, -0.0056081535, 0.023298996, -0.025444955, 0.0097301295, 0.009736794, 0.015274971, -0.0012937407, -0.018087378, -0.0039387033, 0.008637156, -0.011189649, -0.00023846315, -0.011582852, 0.0066411467, -0.018220667, 0.0060846633, 0.0376676, -0.002709108, 0.0072776037, 0.0034188742, -0.010249958, -0.0007747449, -0.00795738, -0.022192692, 0.03910712, 0.032122757, 0.023898797, 0.0076241563, -0.007397564, -0.003655463, 0.011442899, -0.014115352, -0.00505167, -0.031163072, 0.030336678, -0.006857742, -0.022259338, 0.004048667, 0.02072651, 0.0030156737, -0.0042119464, 0.00041861215, -0.005731446, 0.011103011, 0.013822115, 0.021512916, 0.009216965, -0.006537847, -0.027057758, -0.04054665, 0.010403241, -0.0056281467, -0.005701456, -0.002709108, -0.00745088, -0.0024841821, 0.009356919, -0.022659205, 0.004061996, -0.013175662, 0.017074378, -0.006141311, -0.014541878, 0.02993681, -0.00028448965, -0.025271678, 0.011689484, -0.014528549, 0.004398552, -0.017274313, 0.0045751603, 0.012455898, 0.004121976, -0.025458284, -0.006744446, 0.011822774, -0.015035049, -0.03257594, 0.014675168, -0.0039187097, 0.019726837, -0.0047251107, 0.0022825818, 0.011829439, 0.005391558, -0.016781142, -0.0058747325, 0.010309938, -0.013049036, 0.01186276, -0.0011246296, 0.0062112883, 0.0028190718, -0.021739509, 0.009883412, -0.0073175905, -0.012715813, -0.017181009, -0.016607866, -0.042492677, -0.0014478565, -0.01794076, 0.012302616, -0.015194997, -0.04433207, -0.020606548, 0.009696807, 0.010303274, -0.01694109, -0.004018677, 0.019353628, -0.001991011, 0.000058938927, 0.010536531, -0.17274313, 0.010143327, 0.014235313, -0.024152048, 0.025684876, -0.0012504216, 0.036601283, -0.003698782, 0.0007310093, 0.004165295, -0.0029157067, 0.017101036, -0.046891227, -0.017460918, 0.022965772, 0.020233337, -0.024072073, 0.017220996, 0.009370248, 0.0010363255, 0.0194336, -0.019606877, 0.01818068, -0.020819811, 0.007410893, 0.0019326969, 0.017887443, 0.006651143, 0.00067394477, -0.011889419, -0.025058415, -0.008543854, 0.021579562, 0.0047484366, 0.014062037, 0.0075508473, -0.009510202, -0.009143656, 0.0046817916, 0.013982063, -0.0027990784, 0.011782787, 0.014541878, -0.015701497, -0.029350337, 0.021979429, 0.01332228, -0.026244693, -0.0123492675, -0.003895384, 0.0071576433, -0.035454992, -0.00046984528, 0.0033522295, 0.039347045, 0.0005119148, 0.00476843, -0.012995721, 0.0024042083, -0.006931051, -0.014461905, -0.0127558, 0.0034555288, -0.0074842023, -0.030256703, -0.007057676, -0.00807734, 0.007804097, -0.006957709, 0.017181009, -0.034575284, -0.008603834, -0.005008351, -0.015834786, 0.02943031, 0.016861115, -0.0050849924, 0.014235313, 0.0051449724, 0.0025924798, -0.0025741523, 0.04289254, -0.002104307, 0.012969063, -0.008310596, 0.00423194, 0.0074975314, 0.0018810473, -0.014248641, -0.024725191, 0.0151016945, -0.017527562, 0.0018727167, 0.0002830318, 0.015168339, 0.0144219175, -0.004048667, -0.004358565, 0.011836103, -0.010343261, -0.005911387, 0.0022825818, 0.0073175905, 0.00403867, 0.013188991, 0.03334902, 0.006111321, 0.008597169, 0.030123414, -0.015474904, 0.0017877447, -0.024551915, 0.013155668, 0.023525586, -0.0255116, 0.017220996, 0.004358565, -0.00934359, 0.0099967085, 0.011162991, 0.03092315, -0.021046404, -0.015514892, 0.0011946067, -0.01816735, 0.010876419, -0.10124666, -0.03550831, 0.0056348112, 0.013942076, 0.005951374, 0.020419942, -0.006857742, -0.020873128, -0.021259667, 0.0137554705, 0.0057880944, -0.029163731, -0.018767154, -0.021392956, 0.030896494, -0.005494857, -0.0027307675, -0.006801094, -0.014821786, 0.021392956, -0.0018110704, -0.0018843795, -0.012362596, -0.0072176233, -0.017194338, -0.018713837, -0.024272008, 0.03801415, 0.00015880188, 0.0044951867, -0.028630573, -0.0014070367, -0.00916365, -0.026537929, -0.009576847, -0.013995391, -0.0077107945, 0.0050016865, 0.00578143, -0.04467862, 0.008363913, 0.010136662, -0.0006268769, -0.006591163, 0.015341615, -0.027377652, -0.00093136, 0.029243704, -0.020886457, -0.01041657, -0.02424535, 0.005291591, -0.02980352, -0.009190307, 0.019460259, -0.0041286405, 0.004801752, 0.0011787785, -0.001257086, -0.011216307, -0.013395589, 0.00088137644, -0.0051616337, 0.03876057, -0.0033455652, 0.00075850025, -0.006951045, -0.0062112883, 0.018140694, -0.006351242, -0.008263946, 0.018154023, -0.012189319, 0.0075508473, -0.044358727, -0.0040153447, 0.0093302615, -0.010636497, 0.032789204, -0.005264933, -0.014235313, -0.018393943, 0.007297597, -0.016114693, 0.015021721, 0.020033404, 0.0137688, 0.0011046362, 0.010616505, -0.0039453674, 0.012109346, 0.021099718, -0.0072842683, -0.019153694, -0.003768759, 0.039320387, -0.006747778, -0.0016852784, 0.018154023, 0.0010963057, -0.015035049, -0.021033075, -0.04345236, 0.017287642, 0.016341286, -0.008610498, 0.00236922, 0.009290274, 0.028950468, -0.014475234, -0.0035654926, 0.015434918, -0.03372223, 0.004501851, -0.012929076, -0.008483873, -0.0044685286, -0.0102233, 0.01615468, 0.0022792495, 0.010876419, -0.0059647025, 0.01895376, -0.0069976957, -0.0042952523, 0.017207667, -0.00036133936, 0.0085905045, 0.008084005, 0.03129636, -0.016994404, -0.014915089, 0.020100048, -0.012009379, -0.006684466, 0.01306903, 0.00015765642, -0.00530492, 0.0005277429, 0.015421589, 0.015528221, 0.032202728, -0.003485519, -0.0014286962, 0.033908837, 0.001367883, 0.010509873, 0.025271678, -0.020993087, 0.019846799, 0.006897729, -0.010216636, -0.00725761, 0.01818068, -0.028443968, -0.011242964, -0.014435247, -0.013688826, 0.006101324, -0.0022509254, 0.013848773, -0.0019077052, 0.017181009, 0.03422873, 0.005324913, -0.0035188415, 0.014128681, -0.004898387, 0.005038341, 0.0012320944, -0.005561502, -0.017847456, 0.0008538855, -0.0047884234, 0.011849431, 0.015421589, -0.013942076, 0.0029790192, -0.013702155, 0.0001199605, -0.024431955, 0.019926772, 0.022179363, -0.016487904, -0.03964028, 0.0050849924, 0.017487574, 0.022792496, 0.0012504216, 0.004048667, -0.00997005, 0.0076041627, -0.014328616, -0.020259995, 0.0005598157, -0.010469886, 0.0016852784, 0.01716768, -0.008990373, -0.001987679, 0.026417969, 0.023792166, 0.0046917885, -0.0071909656, -0.00032051947, -0.023259008, -0.009170313, 0.02071318, -0.03156294, -0.030869836, -0.006324584, 0.013795458, -0.00047151142, 0.016874444, 0.00947688, 0.00985009, -0.029883493, 0.024205362, -0.013522214, -0.015075036, -0.030603256, 0.029270362, 0.010503208, 0.021539574, 0.01743426, -0.023898797, 0.022019416, -0.0068777353, 0.027857494, -0.021259667, 0.0025758184, 0.006197959, 0.006447877, -0.00025200035, -0.004941706, -0.021246338, -0.005504854, -0.008390571, -0.0097301295, 0.027244363, -0.04446536, 0.05216949, 0.010243294, -0.016008062, 0.0122493, -0.0199401, 0.009077012, 0.019753495, 0.006431216, -0.037960835, -0.027377652, 0.016381273, -0.0038620618, 0.022512587, -0.010996379, -0.0015211658, -0.0102233, 0.007071005, 0.008230623, -0.009490209, -0.010083347, 0.024431955, 0.002427534, 0.02828402, 0.0035721571, -0.022192692, -0.011882754, 0.010056688, 0.0011904413, -0.01426197, -0.017500903, -0.00010985966, 0.005591492, -0.0077707744, -0.012049366, 0.011869425, 0.00858384, -0.024698535, -0.030283362, 0.020140035, 0.011949399, -0.013968734, 0.042732596, -0.011649498, -0.011982721, -0.016967745, -0.0060913274, -0.007130985, -0.013109017, -0.009710136], 
      'numCandidates': 150, 
      'limit': 10
    }
  }, {
    '$project': {
      '_id': 0, 
      'plot': 1, 
      'title': 1, 
      'score': {
        '$meta': 'vectorSearchScore'
      }
    }
  }
]

# run pipeline
result = client["sample_mflix"]["embedded_movies"].aggregate(pipeline)
# print results
for i in result:
    print(i)
 