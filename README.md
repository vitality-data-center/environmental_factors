# environmental_factors
development of environmental factors at 4 and 6 digit postcode level of the Netherlands. All these factors are developed in line with our research plan (https://drive.google.com/file/d/1Nhf7pXeVYyrci7rVWXKwKhQQnDwtCinI/view?usp=sharing), and will be linked to travel behavior variables. 



#### Street length/street density (from OPenStreetMap)
#### Residential Building area/building density (from OpenStreetMap) 


#### air pollution indicators

#### noise pollution
The noise pollution data is provided by RIVM (Rijksinstituut voor Volksgezondheid en Milieu), and can be downloaded as GIS file from https://www.atlasnatuurlijkkapitaal.nl/kaarten. Some description of the noise data can be found below. 

1 = zeer goed 		Lden<=45 dB
2 = goed 			45<Lden<=50 dB
3 = redelijk 		50<Lden<=55 dB
4 = matig 			55<Lden<=60 dB
5 = slecht 			60<Lden<=65 dB
6,7,8 = zeer slecht 		Lden>65 dB

De geluidklassen hebben betrekking op de cumulatieve geluidbelasting in Lden (jaar) als veroorzaakt door
- rijkswegen (2016)
- gemeentelijke en provinciale wegen (2011)
- railverkeer (2016)
- luchtvaart (2011)
- industrie (kentalraming)
- windturbines (2015)

For pc4, indicator for noise level 1 is represented by dn_1, which is the ratio of the area of noise level 1 in the pc4 area. The same for other noise levels 2, 3, 4, 5, 6. 
For pc6, indicator for noise level 1 is represented by dn_1, which is the ratio of the area of noise level 1 in the buffer area of the pc6 centroid. The same for other noise levels 2, 3, 4, 5, 6. 




