# Environmental factors
Following is a list of environmental factors at 4 and 6 digit postcode level of the Netherlands. All these factors are developed in line with our research plan, and will be linked to travel behavior variables. It should be mentioned that the environmental factors of pc6 level are calculated given three different buffer sizes (300 meters, 600 meters, 1000 meters). If you use any of our codes or data for your research, plase cite the following papers:

*Xipei Ren, Zhiyong Wang, Carolin Nast, Dick Ettema, and Aarnout Brombacher. 2019. Integrating Industrial Design and Geoscience: a Survey on Data-Driven Research to Promote Public Health and Vitality. In 9th International Digital Public Health Conference (2019) (DPH’ 19), November 20–23, 2019, Marseille, France. ACM, New York, NY, USA, 5 pages. https://doi.org/10.1145/3357729.3357747* 

*Wang, Z., Ettema, D., & Helbich, M. (2021). Objective environmental exposures correlate differently with recreational and transportation walking: A cross-sectional national study in the Netherlands. Environmental research, 194, 110591.*

#### Number of crossings (representing street connectivity)
The number of crossings witihin the buffers (300, 600, and 1000) around each pc6 centroid. <br>
`crossing_1`: cul-de-sac <br>
`crossing_3`: 3-way crossings <br>
`crossing_4plus`: >=4-way crossings <br>


#### Number of ddresses (representing degree of urbanization)
The number of addresses witihin the buffers (300, 600, and 1000) around each pc6 centroid.<br>
It is indicated by the attribute `addr_num` in the datasets.

#### Street density (from OpenStreetMap)
It is indicated by the attribute `street_density` in the datasets. <br>
For pc4, its value is the total length of all walking streets divided by the area of pc4. <br>
For pc6, its value is the total length of all walking streets divided by the buffer area around the pc6 centroid.

<img width="600" height="700"  src="https://github.com/vitality-data-center/environmental_factors/blob/master/images/road.png" />


#### Residential building density (from OpenStreetMap) 
It is indicated by the attribute `res_bldg_density` in the datasets.<br>
For pc4, its value is the total area of all residential buildings divided by the area of pc4. <br>
For pc6, its value is the total length of all residential buildings divided by the buffer area around the pc6 centroid.<br>

#### air pollution indicators (from European Environment Agency https://www.eea.europa.eu/)
Totally 4 air pollution indicators are generated: NO2 (1000m x 1000m), PM25 (1000m x 1000m), PM10 (1000m x 1000m), NOx (2000m x 2000m), which are indicated by the attributes `no2_avg`, `pm25_avg`,`pm10_avg`,`nox_avg`, respectively. <br>
For pc4 and each type of air pollution, their values are the average values of all cells in each pc4. <br>
For pc6 and each type of air pollution, their values are the average values of all cells in the buffer around pc6 centroid.<br>
<img width="600" height="700"  src="https://github.com/vitality-data-center/environmental_factors/blob/master/images/air.png" />


#### noise pollution
The noise pollution data is provided by RIVM (Rijksinstituut voor Volksgezondheid en Milieu), and can be downloaded as GIS file from https://www.atlasnatuurlijkkapitaal.nl/kaarten. Some description of the noise data can be found below. 
<img width="650"  src="https://github.com/vitality-data-center/environmental_factors/blob/master/images/noise_map.png" />

1 = zeer goed 		Lden<=45 dB <br>
2 = goed 			45<Lden<=50 dB <br>
3 = redelijk 		50<Lden<=55 dB <br>
4 = matig 			55<Lden<=60 dB <br>
5 = slecht 			60<Lden<=65 dB <br>
6,7,8 = zeer slecht 		Lden>65 dB <br>

De geluidklassen hebben betrekking op de cumulatieve geluidbelasting in Lden (jaar) als veroorzaakt door
- rijkswegen (2016)
- gemeentelijke en provinciale wegen (2011)
- railverkeer (2016)
- luchtvaart (2011)
- industrie (kentalraming)
- windturbines (2015)

For pc4, indicator for noise level 1 is represented by the attribute `dn_1`, which is the ratio of the area of noise level 1 in the pc4 area. The same for other noise levels 2, 3, 4, 5, 6. 
For pc6, indicator for noise level 1 is represented by `dn_1`, which is the ratio of the area of noise level 1 in the buffer area of the pc6 centroid. The same for other noise levels 2, 3, 4, 5, 6. 


#### Landuse mix entropy (derived from Bestand Bodemgebruik)
It is represented by the attribute `landuse_idx`. <br>
For pc4, it is calcuated based on the following three land use classification:<br>
- Group 1, residential, 20
- Group 2, recreational 40, 41, 42, 43, 44, 50, 51, 60, 61, 62, 70, 71, 72, 73, 74, 75, 76, 77, 78, 80, 81, 82, 83
- Group 3, other: 10, 11, 12, 21, 22, 23, 24, 30, 31, 32, 33, 34, 35

For pc6, it is calcuated based on the following five land use classification:<br>
- Group 1, residential, 20
- Group 2, recreational 40, 41, 42, 43, 44, 50, 51, 60, 61, 62, 70, 71, 72, 73, 74, 75, 76, 77, 78, 80, 81, 82, 83
- Group 3, other: 10, 11, 12, 22, 23, 30, 31, 32, 33, 34, 35
- Group 4, commercial, 21
- Group 5, industrial, 24

<img width="600"  src="https://github.com/vitality-data-center/environmental_factors/blob/master/images/landuse1.png" />
<img width="618"  src="https://github.com/vitality-data-center/environmental_factors/blob/master/images/landuse2.png" />

#### NDVI (Normalized Difference Vegetation Index)
The details of ndvi can be found via the following link:
https://gisgeography.com/ndvi-normalized-difference-vegetation-index/. <br>
For pc4, their values are the average values of all cells of in each pc4. <br>
For pc6, their values are the average values of all cells in the buffer around pc6 centroid.<br>
Note that negative values are excluded from the calculation.

<img width="600" height="700"  src="https://github.com/vitality-data-center/environmental_factors/blob/master/images/ndvi.png" />



