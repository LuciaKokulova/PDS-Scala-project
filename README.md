# PDS-Scala-project
Cieľom tohto projektu je vypracovať zadanie využívajúce dataset [New York City Taxi Fare Dataset](https://www.kaggle.com/c/new-york-city-taxi-fare-prediction/data) pomocou programu v jazyku Scala. Tento dataset obsahuje viac ako 55 miliónov riadkov, pričom jeden riadkov obsahuje údaje o jednej jazde taxíkom vo formáte 

*key*, // identifikátor jazdy zložený z datetime informácií a jedinečného čísla
<br/>*fare_amount*, // suma zaplatená za jazdu
<br/>*pickup_datetime*, // dátum a čas začiatku jazdy
<br/>*pickup_longitude*, // súradnice začiatku jazdy
<br/>*pickup_latitude*,
<br/>*dropoff_longitude*, // súradnice konca jazdy
<br/>*dropoff_latitude*,
<br/>*passenger_count*. // počet pasažierov v taxíku

Zadanie projektu je rozdelené do dvoch častí. V prvej z nich vyrátame priemernú sumu zaplatenú jedným pasažierom za jeden kilometer jazdy v jednotlivých rokoch, ktoré obsahuje dataset. V druhej časti zadania ukážeme kedy a odkiaľ, resp. kam pasažieri najčastejšie cestujú. 
