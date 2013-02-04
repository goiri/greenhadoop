#!/usr/bin/env python2.5

"""
GreenHadoop makes Hadoop aware of solar energy availability.
http://www.research.rutgers.edu/~goiri/
Copyright (C) 2012 Inigo Goiri, Rutgers University

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program.  If not, see <http://www.gnu.org/licenses/>
"""

from datetime import datetime,timedelta

import sys
sys.path.append(GREEN_PREDICTOR)
import model_cloudy,setenv

dateHighHigh =  datetime(2011, 5, 9, 0, 0, 0) # Fewer energy OK
dateHighMed = 	datetime(2011, 5, 12, 0, 0, 0) # Worst for us
dateMedHigh = 	datetime(2011, 6, 14, 0, 0, 0) # Worst for u
dateMedMed =  	datetime(2011, 6, 16, 0, 0, 0) # More energy

baseDate = dateHighHigh

TOTALTIME = 60*60
speedup = 24

ep = model_cloudy.CachedEnergyPredictor(baseDate, predictionHorizon=(TOTALTIME*speedup/3600), path=GREEN_PREDICTOR, threshold=2, scalingFactor=2375, useActualData=True, error_exit=model_cloudy.normal)
#ep = model_cloudy.CachedEnergyPredictor(BASE_DATE,predictionHorizon=TOTALTIME/3600,path='./greenavailability',threshold=2, scalingFactor=MAX_POWER,scalingBase=BASE_POWER,error_exit=exitType) # Threshold = 2hour; 

date = baseDate + timedelta(seconds=0)

prediction, flag = ep.getGreenAvailability(date, int(TOTALTIME*speedup/3600)) # 48h
print prediction