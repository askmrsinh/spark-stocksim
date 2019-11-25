```
This project uses Spark framework to run Monte Carlo simulation on historical stock prices.
Historical data is read from CSV files and dift for a symbol is calculated. We then use random distribution on the last
known price to estimate future prices for a set number of days throughout a number of iterations.
A a broker class then selects one of these simulations for each stock symbol at random and invests money (dynamically) 
on them. Profits and losses are recorded at the end of the simulation in a tabular format.

The scripts in https://github.com/user501254/BD_STTP_2016.git can be used for semi-automated 
local Hadoop and Spark setup or for setup on AWS Amazon Elastic Compute Cloud (EC2), Google Compute Engine (GCE).

-----
INDEX
-----
1. About the Alpha Vantage API
2. Some Important Files
3. Application Design
4. Setup Instructions
5. Usage Details
6. Visualization and Demo
7. Map Reduce Implementation


------------------------------
1. About the Alpha Vantage API
------------------------------

Alpha Vantage APIs are grouped into four categories: (1) Stock Time Series Data, 
(2) Physical and Digital/Crypto Currencies (e.g., Bitcoin), (3) Technical Indicators, and (4) Sector Performances.
For this project, we use the Stock Time Series Data to get historical prices for a given stock.

API Documentation:
    https://www.alphavantage.co/documentation/

Get Free API Key:
    https://www.alphavantage.co/support/#api-key

Sample data for MSFT symbol:
    timestamp,open,high,low,close,volume
    2019-11-20,150.3100,150.8400,148.4600,149.6200,23118304
    2019-11-19,150.8800,151.3300,150.2000,150.3900,23935700
    2019-11-18,150.0700,150.5500,148.9800,150.3400,21534000
    .
    .
    .
    1999-11-23,89.2500,91.3750,88.3750,89.6250,70787400
    1999-11-22,89.6250,90.3718,88.4380,89.8130,90596600


-----------------------
2. Some Important Files
-----------------------

hw3/src/main/resources/bin
    downloadsymboldata.sh                   bash script to download historical data for a given stock symbol (eg. MSFT)
    downloadsymbolslist.sh                  bash script to download a list of known symbols on NASDQ NASDAQ

hw3/src/main/scala/com/ashessin/cs441/hw3/stocksim/
    InvestmentBroker.scala                  for selecting a random simuation path and making investments
    MontecarloSimulation                    core to Monte Carlo Simulation on stock data obtained from CSV file
    RunMontecarloSimulation.scala           utility object file to connect above two classes


---------------------
3. Application Design
---------------------

The simulation approach is from the book "Python for Finance: Analyze Big Financial Data, Yves Hilpisch".
Also, https://www.youtube.com/watch?v=3gcLRU24-w0&feature=youtu.be&t=204.

First we read the historical (closing) prices for a symbol from CSV into a DataFrame. Additional columns are computed on
the fly and added to the dataframe. These are:
    1. change      - daily stock price change for a given symbol    (price(n) - price(n-1))
    2. pct_change  - daily percentage change for a given symbol     (price(n) - price(n-1)) / price(n-1)
    3. log_returns - natural log of pct_change + 1                  ln(((price(n) - price(n-1)) / price(n-1)) + 1)
    
    Table: Historical data for MSFT symbol with additional columns
    +----------+------+-----------+--------------------+--------------------+
    | timestamp| close|     change|          pct_change|         log_returns|
    +----------+------+-----------+--------------------+--------------------+
    |1999-11-22|89.813|       null|                null|                null|
    |1999-11-23|89.625|-0.18800354|-0.00209327750580...|-0.00209547147341...|
    |1999-11-24|91.688|  2.0630035|0.023018170600156904| 0.02275724888423533|
    |1999-11-26|91.125|-0.56300354|-0.00614042751834165|-0.00615935747519754|
    |1999-11-29|90.188|-0.93699646|-0.01028254002700...|-0.01033577055278...|
    +----------+------+-----------+--------------------+--------------------+
    only showing top 5 rows


We will use data in `log_returns` column to calculate overall stock price drift and standard deviation for use in our
Monte Carlo simulation. Using logarithmic return is better, see why:
    https://quantivity.wordpress.com/2011/02/21/why-log-returns/

Next we calculate the variance, stddev, mean and drift for stock returns and use these values to estimate the expected
returns over a random distribution for a set number of days (rows) and iterations (array size in `valueArray` column).

Here, the drift is defined as:
    drift = mean(log_returns) - 0.5 * variance(log_returns)

And expected return (Z Score) is calculated as:
     exp(drift + deviation * distribution.inverseCumulativeProbability(value)

2019-11-25 00:46:53,530 -0600 [ForkJoinPool-1-worker-5] INFO  (RunMontecarloSimulation.scala:46) - MSFT stock returns variance: 4.511899231076514E-4
2019-11-25 00:46:53,531 -0600 [ForkJoinPool-1-worker-5] INFO  (RunMontecarloSimulation.scala:47) - MSFT stock returns deviation: 0.021241231675862192
2019-11-25 00:46:53,531 -0600 [ForkJoinPool-1-worker-5] INFO  (RunMontecarloSimulation.scala:48) - MSFT stock returns mean: 1.0144483073271711E-4
2019-11-25 00:46:53,536 -0600 [ForkJoinPool-1-worker-5] INFO  (RunMontecarloSimulation.scala:49) - MSFT stock returns drift: -1.241501308211086E-4

    Table: Expected future returns (Z scores)
    +--------------------+
    |          valueArray|
    +--------------------+
    |[1.05515136005395...|
    |[1.11660235627113...|
    |[0.98645836268787...|
    |[0.92650393370421...|
    |[1.04098867890015...|
    +--------------------+
    only showing top 5 rows


We then take the last known closing price and multiply it with our Expected future returns.

    Table: Expected future stock prices for MSFT (valueArray)
    +--------------------+
    |          valueArray|
    +--------------------+
    |[149.619995117187...|
    |[147.738231038638...|
    |[152.448257240264...|
    |[150.628174249042...|
    |[151.305624221947...|
    +--------------------+
    only showing top 5 rows


Since at this point the values for each iteration on a given day is stored as an `Row[Array[Double]]`, we explode it.

    Table: Expected future stock prices for MSFT (for x days(rows), and 6 iterations(columns)) 
    +------------------+------------------+------------------+------------------+------------------+------------------+
    |               c_1|               c_2|               c_3|               c_4|               c_5|               c_6|
    +------------------+------------------+------------------+------------------+------------------+------------------+
    | 149.6199951171875| 149.6199951171875| 149.6199951171875| 149.6199951171875| 149.6199951171875| 149.6199951171875|
    | 147.7382310386384|149.57309488489616|151.38360286904722|143.93718738358982|148.10418911170942|151.51272810879016|
    |152.44825724026444|146.94372665656476|148.17021606888645|144.78228352084327|152.19283708684793| 153.2640550593828|
    |150.62817424904205|144.39749699725647|149.00135167825218|144.68636798388823|149.40161433218884|147.66921778183877|
    | 151.3056242219476|143.73321953105386|156.29962733093882| 149.1660503436783|146.19719683832446|147.26748850075634|
    +------------------+------------------+------------------+------------------+------------------+------------------+
    only showing top 5 rows


The above process is done for each stock symbol (eg. AMZN,AAPL,MSFT) in the portfolio.
Finally, we make use of a dummy investment stratagy to buy and sell stocks on each simulation day. On the first day, 
equal amount of money is allocated for buying each of the symbols in the portfolio. There after, stocks are bought/sold 
when the mean percentage change in the past days of simulation is greater/less than 0 respectively for each symbol.
Buying happens incrementally, where as selling is cumulative.
On the last day, all stocks are sold and profit/loss is booked.
    
    Table: Investment details for each simulation day (some columns hidden and values truncated for display)
    +---+-----------+------------------+-+--------------------+--------------+-------------+------------------+-------+
    | id|stockSymbol|    predictedValue|.|     mean_pct_change|quantityBought|   marketCost|    remainingFunds|outlook|
    +---+-----------+------------------+-+--------------------+--------------+-------------+------------------+-------+
    |  1|       AMZN| 1745.530029296875|.|                 0.0|           1.0| 1745.587...5| 8254.469970703125|    BUY|
    |  1|       AAPL|263.19000244140625|.|                 0.0|          12.0| 3158.287...5|  5096.18994140625|    BUY|
    |  1|       MSFT| 149.6199951171875|.|                 0.0|          22.0| 3291.612...5| 1804.550048828125|    BUY|
    |  2|       AMZN|1821.1505181370044|.|0.021661182440553723|           0.0|          0.0| 1804.550048828125|    BUY|
    |  2|       AAPL|245.86861972824198|.|-0.03290661224303...|         -12.0|2950.4203...5| 4754.973485567029|   SELL|
    |  2|       MSFT|151.51272810879016|.|0.006325133850325965|          10.0|1515.1201...6|3239.8462044791268|    BUY|
    |  3|       AMZN|1842.0135754749651|.|0.011039052943434985|           0.0|          0.0|3239.8462044791268|    BUY|
    |  3|       AAPL|236.26625334697493|.|-0.02398715993518...|          -0.0|          0.0|3239.8462044791268|   SELL|
    |  3|       MSFT| 153.2640550593828|.|0.005961358867846343|           7.0|1072.8479...6|2166.9978190634474|    BUY|
    |  4|       AMZN| 1907.220146046128|.|0.011609666873831307|           0.0|          0.0|2166.9978190634474|    BUY|
    |  4|       AAPL|247.32655790614575|.|0.005706430855499625|           2.0| 494.6591...5|1672.3447032511558|    BUY|
    |  4|       MSFT|147.66921778183877|.|-0.00763580090905...|         -39.0| 5759.071...2|7431.4441967428675|   SELL|
    |  5|       AMZN|1972.7715829167523|.|0.009195962784168149|           1.0|1972.7752...3| 5458.672613826115|    BUY|
    |  5|       AAPL|244.26782556068173|.|-0.00133215005941...|          -2.0|488.53534...6| 5947.208264947478|   SELL|
    |  5|       MSFT|147.26748850075634|.|-0.00207125364572...|          -0.0|          0.0| 5947.208264947478|   SELL|
    |  6|       AMZN| 1994.071770222609|.|0.003332175039554328|           0.0|          0.0| 5947.208264947478|    BUY|
    |  6|       AAPL| 253.2189474651591|.|0.005885425479420125|           7.0|1772.5313...6| 4174.675632691364|    BUY|
    |  6|       MSFT|144.31455586087606|.|-0.00368712401664...|          -0.0|          0.0| 4174.675632691364|   SELL|
    |  7|       AMZN|1994.2138320221031|.|4.862024441400062E-4|           0.0|          0.0| 4174.675632691364|    BUY|
    |  7|       AAPL| 237.6563380159748|.|-0.00793909683117...|          -7.0|1663.5923...5| 5838.269998803187|   SELL|
    |  7|       MSFT|143.54340814327054|.|-0.00129009200442...|          -0.0|          0.0| 5838.269998803187|   SELL|
    |  8|       AMZN|2019.9112511464944|.|0.001671524032143...|           0.0|          0.0| 5838.269998803187|    BUY|
    |  8|       AAPL|237.96931824156454|.|-8.27769031745765...|          -0.0|          0.0| 5838.269998803187|   SELL|
    |  8|       MSFT|142.29968570007847|.|-0.00124431580036...|          -0.0|          0.0| 5838.269998803187|   SELL|
    |  9|       AMZN|1995.7709519117618|.|-0.00114218269209...|          -2.0|3991.5423...6|  9829.81190262671|   SELL|
    |  9|       AAPL|244.51648211516803|.|0.002964985518292...|          13.0| 3178.718...4| 6651.097635129527|    BUY|
    |  9|       MSFT|142.50286804611562|.|2.039248568589026...|          15.0|2137.5434...5| 4513.554614437792|    BUY|
    | 10|       AMZN|1980.7407165829306|.|-8.67322492639214...|          -0.0|          0.0| 4513.554614437792|   SELL|
    | 10|       AAPL| 238.2054404982603|.|-0.00228453057238...|         -13.0|3096.6783...6| 7610.225340915176|   SELL|
    | 10|       MSFT|145.24852981758028|.|0.001928780660240476|          17.0|2469.2264...6| 5141.000334016311|    BUY|
    | 11|       AMZN|1770.6761004314574|.|-0.00972008076455211|          -0.0|       0....0| 5141.000334016311|   SELL|
    | 11|       AAPL|243.07697107036714|.|0.001651493827317...|           7.0|  1701.25...7|3439.4615365237414|    BUY|
    | 11|       MSFT| 143.5419340713539|.|-8.92791506315769...|         -32.0| 4593.332...5| 8032.803426807066|   SELL|
    | 12|       AMZN| 1834.739585534063|.|0.002205013214403...|           1.0| 1834.706...3| 6198.063841273003|    BUY|
    | 12|       AAPL|241.70242201788227|.|-3.33607955165453...|          -7.0|1691.9175...8| 7889.980795398179|   SELL|
    | 12|       MSFT|143.75985101142336|.|5.211248377878655E-5|          18.0|2587.6720...4| 5302.303477192559|    BUY|
    | 13|       AMZN|1879.1428802094172|.|0.002031263731814039|           0.0|          0.0| 5302.303477192559|    BUY|
    | 13|       AAPL|258.35531059537425|.|0.005274207906109...|           6.0|1550.1345...5| 3752.171613620313|    BUY|
    | 13|       MSFT|140.40340522632485|.|-0.00179195966191...|         -18.0|2527.2647...2|  6279.43290769416|   SELL|
    | 14|       AMZN|1905.9185583861854|.|0.001162867286783766|           1.0|1905.9185...4| 4373.514349307975|    BUY|
    | 14|       AAPL|263.62091737609353|.|0.001832533428249...|           5.0|1318.1067...7|3055.4097624275073|    BUY|
    | 14|       MSFT| 148.2504573042822|.|0.003864097794877501|           6.0| 889.5093...2|2165.9070186018143|    BUY|
    | 15|       AMZN|1823.1254368937944|.|-0.00281847618986...|          -2.0|3646.2588...7| 5812.157892389403|   SELL|
    | 15|       AAPL|279.93822874993515|.|0.004248627330622...|           6.0|1679.6210...8| 4132.528519889793|    BUY|
    | 15|       MSFT| 148.4925006079745|.|3.664508398688171...|           9.0|1336.4370...6| 2796.096014418022|    BUY|
    | 16|       AMZN|1771.3343008127158|.|-0.00195164751708...|          -0.0|          0.0| 2796.096014418022|   SELL|
    | 16|       AAPL|291.19639873865896|.|0.002779078096501...|           3.0| 873.5876...9|1922.5068182020452|    BUY|
    | 16|       MSFT|142.51259154580487|.|-0.00249402067290...|         -15.0| 2137.607...3| 4060.195691389118|   SELL|
    | 17|       AMZN|1720.7059778289665|.|-0.00179609844029...|          -0.0|          0.0| 4060.195691389118|   SELL|
    | 17|       AAPL|291.57635599582824|.|2.402289709615675...|           4.0| 1166.331...3|2893.8902674058054|    BUY|
    | 17|       MSFT|142.95215598987642|.|3.472762061568348...|           6.0| 857.7158...5| 2036.177331466547|    BUY|
    | 18|       AMZN| 1775.108971138221|.|0.001656698427771315|           0.0|          0.0| 2036.177331466547|    BUY|
    | 18|       AAPL|315.07294959087864|.|0.004490273911751226|           2.0| 630.1457...3|1406.0314322847898|    BUY|
    | 18|       MSFT|146.29724832856883|.|0.001301933932500...|           3.0| 438.8906...5| 967.1396872990833|    BUY|
    | 19|       AMZN|1798.5537952264597|.|7.823283782589504E-4|           0.0|          0.0| 967.1396872990833|    BUY|
    | 19|       AAPL|345.16677271885436|.|0.005263373720232...|           0.0|          0.0| 967.1396872990833|    BUY|
    | 19|       MSFT| 141.6084479027531|.|-0.00161830977517...|          -9.0| 1274.477...8|2241.6157184238614|   SELL|
    | 20|       AMZN|1840.1019323865196|.|0.001194159355919...|           0.0|          0.0|2241.6157184238614|    BUY|
    | 20|       AAPL| 332.8855455149993|.|-0.00151585933385...|         -26.0| 8655.098...2|10896.639901813844|   SELL|
    | 20|       MSFT|141.75591456319827|.|-2.88470342884638...|          -0.0|          0.0|10896.639901813844|   SELL|
    | 20|       AMZN|1840.1019323865196|.|0.001194159355919...|          -0.0|          0.0|10896.639901813844|   SELL|
    | 20|       AAPL| 332.8855455149993|.|-0.00151585933385...|          -0.0|          0.0|10896.639901813844|   SELL|
    | 20|       MSFT|141.75591456319827|.|-2.88470342884638...|          -0.0|          0.0|10896.639901813844|   SELL|
    +---+-----------+------------------+-+--------------------+--------------+-------------+------------------+-------+
    
    Investment amount: 10000.0
    Amount at the end of simulation: 10896.639901813844
    change: 8.966399018138436%


---------------------
4. Setup Instructions
---------------------

Something to note, absolute paths are almost always preferred. Make sure to use correct file system URI.
For example, if the file 'hw2/src/main/resources/data/' is on a ordinary filesystem, within the users home directory,
use:
    file://$HOME/hw2/src/main/resources/data/

Similarly, for file on HDFS,
    use hdfs://$HOSTNAME:PORT/some-path/

For S3 bucket use,
    s3://bucket-name/some-path/

1. Setup Hadoop and Spark using bootstrap script and start all services
    git clone "https://github.com/user501254/BD_STTP_2016.git"; cd BD_STTP_2016; chmod +x *.sh; 
    InstallHadoop.sh; InstallSpark.sh; start-all.sh

2. Clone this repository
    git clone https://asing80@bitbucket.org/asing80/hw3.git

3. Download the stock data in CSV format
    ./hw3/src/main/resources/bin/downloadsymboldata.sh $API_KEY $SYMBOL

4. Run jar file using `spark-submit`
    spark-submit hw3-assembly-0.1.jar \
      hw3/src/main/resources/data/ \
      MSFT,AAPL,AMZN 10 10 10000

For AWS EMR, please follow these steps:
    https://docs.aws.amazon.com/emr/latest/ReleaseGuide/emr-launch-custom-jar-cli.html
Input S3 bucket locations should be passed to the JAR file and must be accessible.


----------------
6. Usage Details
----------------

java com.ashessin.cs441.hw3.stocksim.Start <option> \
    <absolute_csv_directory_path> <symbols> <period> <iterations> <value>

<option>:
	[--simulate, RunMontecarloSimulation | 
	 --configFile]


<absolute_csv_directory_path>:
    fully qualified csv data directory URI.

    example paths:
        file:///absolute-path-to-input-directory/
        hdfs://localhost:9000/absolute-path-to-input-directory/
        s3://bucket-name/absolute-path-to-input-directory/
        gs://bucket-name/absolute-path-to-input-directory/

<symbols>
    a comma separated list of NASDAQ stock symbols, eg. MSFT,APPAL,AMZN

<period>
	period of simulation

<iterations>
	number of iterations for each day

<value>
	the investment amount to start with

For execution through custom config file, please see src/main/resources/reference.conf for fields.

examples:

	java com.ashessin.cs441.hw3.stocksim.Start --configFile \
    	   file:///absolute-path-to-config-file.conf

	java com.ashessin.cs441.hw3.stocksim.Start \
    	--simulate file:///absolute-path-to-input-directory/ \
	    MSFT,APPAL,AMZN 28 10 10000

	java com.ashessin.cs441.hw3.stocksim.Start \
    	--simulate hdfs://localhost:9000/absolute-path-to-input-directory/ \
	    MSFT,APPAL,AMZN 28 10 10000


-------------------------
6. Visualization and Demo
-------------------------

The program in its default configuration produces a number of tables, which can used for visualization.
Some samples along with demo for sample runs on Google Dataproc are available at:
    https://asing80.people.uic.edu/cs441/hw3/


----------------------------
7. Map Reduce Implementation
----------------------------
TODO
```