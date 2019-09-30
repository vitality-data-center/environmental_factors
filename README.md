# environmental_factors
development of environmental factors at 4 and 6 digit postcode level of the Netherlands. All these factors are developed in line with our research plan (https://drive.google.com/file/d/1Nhf7pXeVYyrci7rVWXKwKhQQnDwtCinI/view?usp=sharing), and will be linked to travel behavior variables. 



#### Street length/street density (from OPenStreetMap)
#### Residential Building area/building density (from OpenStreetMap) 


#### air pollution indicators

#### noise pollution
The noise pollution data is provided by RIVM (Rijksinstituut voor Volksgezondheid en Milieu), and can be downloaded as GIS file from https://www.atlasnatuurlijkkapitaal.nl/kaarten. Some description of the noise data can be found below. ![](https://github.com/vitality-data-center/environmental_factors/blob/master/images/noise_map.png)

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

For pc4, indicator for noise level 1 is represented by dn_1, which is the ratio of the area of noise level 1 in the pc4 area. The same for other noise levels 2, 3, 4, 5, 6. 
For pc6, indicator for noise level 1 is represented by dn_1, which is the ratio of the area of noise level 1 in the buffer area of the pc6 centroid. The same for other noise levels 2, 3, 4, 5, 6. 


#### Landuse mix entropy (derived from Bestand Bodemgebruik)
Land use classification:
-Group 1, residential, 20
-Group 2, recreational 40, 41, 42, 43, 44, 50, 51, 60, 61, 62, 70, 71, 72, 73, 74, 75, 76, 77, 78, 80, 81, 82, 83
-Group 3, other: 10, 11, 12, 20, 21, 22, 23, 24, 30, 31, 32, 33, 34, 35
<img width="600"  src="https://github.com/vitality-data-center/environmental_factors/blob/master/images/landuse1.png" />
<img width="600"  src="https://github.com/vitality-data-center/environmental_factors/blob/master/images/landuse2.png" />





