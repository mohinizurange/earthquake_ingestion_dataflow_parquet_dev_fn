import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions,GoogleCloudOptions
import os
from datetime import datetime,timedelta,timezone



## 1Count the number of earthquakes by region
class CuntNumEqa(beam.DoFn):
    def process(self, eq_dic):
        # print(eq_dic,type(eq_dic))
        region = eq_dic['area']

        yield (region,1)

##2 Find the average magnitude by the region

class FilteringData(beam.DoFn):
    def process(self, eq_dic):
        # print(eq_dic,type(eq_dic))
        region = eq_dic['area']
        mag = eq_dic['mag']
        yield (region, mag)

class RegionWiseAvgMag(beam.DoFn):
    def process(self, ele):
        e_length = len(ele[1])
        e_sum = sum(ele[1])
        e_avg = e_sum/e_length
        region = ele[0]
        yield (region, e_avg)


# 3. Find how many earthquakes happen on the same day.
class CuntNumEqOnSameDay(beam.DoFn):
    def process(self, eq_dic):
        # print(eq_dic,type(eq_dic))
        eq_date = eq_dic['time'].date()
        # print(eq_date)#2024-10-24
        yield (str(eq_date), 1)

# 4. Find how many earthquakes happen on same day and in same region
class CuntNumEqOnSameDayRegion(beam.DoFn):
    def process(self, eq_dic):
        # print(eq_dic,type(eq_dic))
        eq_date = eq_dic['time'].date()
        # print(eq_date)#2024-10-24
        region = eq_dic['area']
        data_region = (eq_date,region)
        yield (str(data_region), 1)


# 5. Find average earthquakes happen on the same day.
class CalculateAverageEarthquakes(beam.DoFn):
    def process(self, elements):
        total_earthquakes = sum(count for date, count in elements)
        unique_days = len(elements)
        # Calculate average
        average = total_earthquakes / unique_days if unique_days > 0 else 0
        yield average

# 6. Find average earthquakes happen on same day and in same region
#use above class
# 7. Find the region name, which had the highest magnitude earthquake last week.
class FilterLastWeekEarthquakes(beam.DoFn):
    def process(self, element):
        earthquake_time = element['time'].date()  # Extract only the date portion #2024-10-24
        current_time = datetime.now().date() #2024-11-11

        # Calculate the date for one week ago
        last_week = current_time - timedelta(weeks=1) ####2024-11-04

        # # Check if the earthquake happened last week
        if earthquake_time >= last_week and earthquake_time <= current_time:
            yield (element['area'],element['mag'])


# 8. Find the region name, which is having magnitudes higher than 5.
class FilterHighMagnitude(beam.DoFn):
    def process(self, element):
        # Check if magnitude is greater than 5
        if element['mag'] is not None and element['mag'] > 5:
            yield element['area']

# 9. Find out the regions which are having the highest frequency and intensity of
# earthquakes.



if __name__ == '__main__':
    os.environ[
        'GOOGLE_APPLICATION_CREDENTIALS'] = r"D:\Mohini Data Science\earthquake_ingestion_dataflow_dev\spark-learning-43150-3d588125392c.json"
    option_obj = PipelineOptions()
    google_cloud = option_obj.view_as(GoogleCloudOptions)
    google_cloud.project = 'spark-learning-43150'
    google_cloud.job_name = f"earthquake-analysis-data12"
    google_cloud.region = "us-central1"
    google_cloud.staging_location = "gs://earthquake_df_temp_bk/stagging_loc/"
    google_cloud.temp_location = "gs://earthquake_df_temp_bk/temp_loc/"

    ## src path
    table_name_src = 'spark-learning-43150.earthquake_db.dataflow_earthquake_data'
    with beam.Pipeline(options=option_obj) as p:
        read_data = (p|'read data from bigquery'>> beam.io.ReadFromBigQuery(table = table_name_src)
                # |'print'>> beam.Map(print)
                )

        # 1. Count the number of earthquakes by region
        num_earthquake_by_region = (read_data|'Count the number of earthquakes'>> beam.ParDo(CuntNumEqa())
                          |'combine per key' >> beam.CombinePerKey(sum)
                          # | 'print' >> beam.Map(print)
                          )


        # 2. Find the average magnitude by the region

        avg_earthquake_by_region = (read_data | 'call class FilteringData' >> beam.ParDo(FilteringData())
                                    | 'group by key' >> beam.GroupByKey() #('Saanichton, Canada', [3.96,10.5])
                                    |'call class RegionWiseAvgMag' >> beam.ParDo(RegionWiseAvgMag())
                                    # | 'print' >> beam.Map(print)
                                    )



        # 3. Find how many earthquakes happen on the same day.
        num_earthquake_on_same_day = (read_data|'Count number of earthquakes on same day'>> beam.ParDo(CuntNumEqOnSameDay())
                          |'Group by Date and Count' >> beam.CombinePerKey(sum)
                          # | 'print' >> beam.Map(print)
                          )


        # 4. Find how many earthquakes happen on same day and in same region
        num_earthquake_on_same_day = (read_data|'Extract Date and Region for Counting'>> beam.ParDo(CuntNumEqOnSameDayRegion())
                          |'Group by Date and Region and Count' >> beam.CombinePerKey(sum)
                          # | 'print' >> beam.Map(print)
                          )

        # 5. Find average earthquakes happen on the same day.

        avg_earthquakes_per_day = (
                read_data
                | "extract Date" >> beam.ParDo(CuntNumEqOnSameDay())
                | "count Earthquakes Per Day" >> beam.CombinePerKey(sum)
                | "collect All Counts" >> beam.combiners.ToList() ##[('2024-10-21', 253), ('2024-10-20', 309), ('2024-10-19', 212), ('2024-10-18', 244), ('2024-10-17', 266), ('2024-10-16', 287), ('2024-10-15', 279), ('2024-10-14', 263), ('2024-10-13', 277), ('2024-10-10', 288), ('2024-10-09', 343), ('2024-10-08', 365), ('2024-10-07', 299), ('2024-10-03', 283), ('2024-10-02', 347), ('2024-10-01', 275), ('2024-09-30', 270), ('2024-09-29', 248), ('2024-09-28', 289), ('2024-09-27', 317), ('2024-09-25', 186), ('2024-10-23', 328), ('2024-10-11', 276), ('2024-10-05', 268), ('2024-10-24', 298), ('2024-10-22', 251), ('2024-10-12', 199), ('2024-09-26', 311), ('2024-10-06', 311), ('2024-10-25', 64), ('2024-10-04', 269)]
                | "calculate Average Earthquakes" >> beam.ParDo(CalculateAverageEarthquakes()) #273.38709677419354
                # | "print " >> beam.Map(print)
        )


        # 6. Find average earthquakes happen on same day and in same region
        avg_earthquakes_per_day_same_region = (
                read_data
                | "extract Date date and region" >> beam.ParDo(CuntNumEqOnSameDayRegion())
                | "per day and same region wise count Earthquakes " >> beam.CombinePerKey(sum) # ("(datetime.date(2024, 9, 26), 'Saanichton, Canada')", 1)
                | "create list" >> beam.combiners.ToList() ##[("(datetime.date(2024, 10, 21), 'Fox, Alaska')", 1), ("(datetime.date(2024, 10, 20), 'Fox, Alaska')", 1)]
                | "calculate average eq" >> beam.ParDo(CalculateAverageEarthquakes()) #1.919157608695652
                # | "print " >> beam.Map(print)
        )

        # # 7. Find the region name, which had the highest magnitude earthquake last week.

        highest_magnitude_region = (
                read_data
                | "filter last week earthquakes" >> beam.ParDo(FilterLastWeekEarthquakes()) ## ('Morton, Washington', 1.06),('Burica, Panama', 5.8),('Toyah, Texas', 1.8),('Toyah, Texas', 1.6)
                | "Top Magnitudes" >> beam.combiners.Top.Of(1, key=lambda x: x[ 1])  # Get the top 1 earthquake with highest magnitude [('Cochrane, Chile', 6.2)]
            # | "Print" >> beam.Map(print)

        )
            #Top.Of(1, key=lambda x: x[1]) sorts the records by magnitude in descending order and returns only the top entry.selects the top 1 element based on the second value (magnitude) of the tuple (region, magnitude), ensuring the highest magnitude is chosen.


        # 8. Find the region name, which is having magnitudes higher than 5.
        region_name_having_mag_higher_than_5 = (
            read_data
            | "filter magnitude above 5" >> beam.ParDo(FilterHighMagnitude())
            | "remove duplicates" >> beam.Distinct()
            # | "Print" >> beam.Map(print)
        )


        # 9. Find out the regions which are having the highest frequency and intensity of earthquakes.

        class FindRegionFrequencyAndIntensity(beam.DoFn):
            def process(self, element):
                # Extract region and magnitude
                region = element['area']
                magnitude = element['mag']

                # Output a tuple with region and its earthquake magnitude
                yield (region, (1, magnitude))  # (count, max magnitude)


        # Define a DoFn to combine the results for frequency and intensity
        class CombineRegionFrequencyAndIntensity(beam.DoFn):
            def process(self, element):
                region, values = element
                count = sum(count for count, mag in values)
                max_mag = max(mag for count, mag in values)
                yield (region, (count, max_mag))


        region_with_highest_freq_and_intensity=(
        read_data

        | "find frequency and intensity by region" >> beam.ParDo(FindRegionFrequencyAndIntensity())#('Vieques, Puerto Rico', (1, 3.37)),('Vieques, Puerto Rico', (1, 3.71))
        | "group by region" >> beam.GroupByKey() ##('Vieques, Puerto Rico', [(1, 3.37), (1, 3.71), (1, 3.76), (1, 3.83), (1, 3.7), (1, 3.73), (1, 3.37), (1, 4.68), (1, 3.71), (1, 3.76), (1, 3.83), (1, 3.7), (1, 3.73), (1, 3.71), (1, 3.76), (1, 3.83), (1, 3.7)])
        | "aggregate frequency and max magnitude" >> beam.ParDo(CombineRegionFrequencyAndIntensity()) ##('Vieques, Puerto Rico', (17, 4.68))
        # | "Print Results" >> beam.Map(print)
    )


















#######################################
########q7Top.Of(1, key=lambda x: x[1]): Top.Of is a combiner that takes two arguments:

# 1: Specifies that we want the single highest entry (top 1).
# key=lambda x: x[1]: Defines the ranking criteria. Here, x[1] refers to the magnitude (second item in the tuple). This means Top.Of will use the earthquake magnitude to rank and select entries.
# Purpose: This line scans all records and picks the one with the highest magnitude based on the key we defined (x[1]), which in this example is the magnitude.
#
# In summary, Top.Of(1, key=lambda x: x[1]) sorts the records by magnitude in descending order and returns only the top entry.
#Top.Of(1, key=lambda x: x[1]) sorts the records by magnitude in descending order and returns only the top entry.selects the top 1 element based on the second value (magnitude) of the tuple (region, magnitude), ensuring the highest magnitude is chosen.



#### Q9Intensity can be understood as the average magnitude of earthquakes in a region.
# Magnitude tells you how strong an earthquake is when it occurs. Higher magnitude means higher intensity.
# the maximum magnitude (max(mag)) can be considered as the highest intensity of the earthquake in a region during the given time period