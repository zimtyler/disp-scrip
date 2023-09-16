import os
import timeit
import polars as pl
from datetime import datetime, timedelta

"""
Including Activity IDs (Process States) that 'should' have prior Customer communication, like 17 or 26,
allows us to reasonably assume a customer reply did occur, it just wasn't captured via 5, 7, or 8.

"""
# Redacted
IdDict = {
    "Store Contact Method" : {
        3 : "A",
        4 : "B",
        35 : "C"
    },
# Redacted
    "Customer Process State" : {
        5 : "A",
        7 : "B",
        8 : "C",
        10 : "D",
        11 : "E",
        17 : "F",
        18 : "G",
        19 : "H",
        26 : "I",
        27 : "J",
        32 : "K",
        33 : "L",
        34 : "M",
        36 : "N"
    }

}

def responseCount_to_csv(directory):
    csv_files = glob.glob(os.path.join(directory, "*.csv"))
    for file in csv_files:

        tic = timeit.default_timer()
        suffix = file[:-4][-8:] # Grab them dates off source file
        
        lf = pl.scan_csv(file, has_header=True)
        lf = lf.select(["ActivityTypeId", "ActivityDate", "Origin"])
        
        # Filter to only ActivityTypesIds we're interested in
        lf = lf.filter(pl.col("ActivityTypeId").is_in([key for sub_dict in IdDict.values() for key in sub_dict.keys()]))
        
        # While not optimal to sort minimally filtered lf, it allows us to call .first()
        lf = lf.sort("ActivityDate")

        lf_UniqOL = lf.filter(pl.col("ActivityTypeId").is_in(IdDict["Dealer"].keys()))
        lf_UniqOL = lf_UniqOL.groupby("Origin").agg([
            pl.min("ActivityDate").alias("FirstResponseDate"),
            pl.first("ActivityTypeId").alias("FirstResponseType")
        ])
        
        # Join to primary lf
        lf = lf.join(lf_UniqOL, on="Origin", how="left", validate="m:1")

        # Allows us to see if the customer contacted dealer first. Join after we've filtered to the appropriate timeframe
        lf_CustFirst = lf.with_columns(
            pl.when(pl.col("ActivityDate") < pl.col("FirstResponseDate"))
            .then(pl.col("ActivityTypeId"))
            .otherwise(None)
            .alias("CustContactMethodIfB4Store")
        )

        lf_CustFirst = lf_CustFirst.filter(pl.col("CustContactMethodIfB4Store") != None)
        lf_CustFirst = lf_CustFirst.select(
            pl.col("Origin"), 
            pl.col("ActivityDate").alias("CustContactB4DealerDate"), 
            pl.col("CustContactMethodIfB4Dealer")
        )
        
        lf_CustFirst = lf_CustFirst.groupby("Origin").agg([
            pl.min("CustContactB4StoreDate"),
            pl.count("CustContactMethodIfB4Store").alias("#CustContactB4Store"),
            pl.first("CustContactMethodIfB4Store")
        ])
        lf_CustFirst = lf_CustFirst.with_columns(pl.col("CustContactB4StoreDate").str.strptime(pl.datatypes.Date, "%Y-%m-%dT%H:%M:%S%.f%z", strict=False))

        """
        Filter lf to only Leads with Store Response prior to any Customer Reply (removes Customer contact first)
        
        Note on above: 
            This allows us to assess customer contact AFTER Stores's first quality response. Since, further down,
            we are filtering to any and all activity prior and up to the customers first reply. If the customer
            reaches out first and we only filter based on their first response before doing the step above, then
            it removes situations where a store responds and the customer replies to said dealer (since it isn't
            their first reply)
        
        Additionally, filter lf to only activity ID's in dict

        """
        lf = lf.filter(pl.col("ActivityDate") >= pl.col("FirstResponseDate"))


        # Create Customer Reply table and join FirstReplyDate to original lf
        lf_reply = lf.filter(pl.col("ActivityTypeId").is_in(IdDict["Customer"].keys()))
        lf_reply = lf_reply.groupby("Origin").agg([
            pl.min("ActivityDate").alias("CustomerReplyDate"),
            pl.first("ActivityTypeId").alias("CustomerReplyType")
        ])
        
        # Join to primary lf
        lf = lf.join(lf_reply, on="Origin", how="left", validate="m:1")

        ### Step discussed above ###
        # Filter joined df to all Dispo Activity prior to and including first Customer Reply to Dealer
        lf = lf.filter(
            (pl.col("ActivityDate") <= pl.col("CustomerReplyDate")) |
            (pl.col("CustomerReplyDate") == None)
        )

        """
        Now that we have the proper order established, we want more detail about the Dealer Responses and 
        Method of contact

        """
        lf_response = lf.filter(pl.col("ActivityTypeId").is_in(IdDict["Dealer"].keys()))
        lf_response = lf_response.groupby("Origin").agg([
            pl.max("ActivityDate").alias("ResponseDateBeforeReply"),
            pl.first("ActivityTypeId").alias("FirstResponseType"),
            pl.last("ActivityTypeId").alias("ResponseTypeBeforeReply")
        ])

        lf_response = lf_response.with_columns(pl.col("ResponseDateBeforeReply").str.strptime(pl.datatypes.Date, "%Y-%m-%dT%H:%M:%S%.f%z", strict=False))
        
        # Change datetime to Date & Remove any erroneous dates (e.g. future dates)
        lf = lf.with_columns(pl.col("FirstResponseDate").str.strptime(pl.datatypes.Date, "%Y-%m-%dT%H:%M:%S%.f%z", strict=False))
        lf = lf.with_columns(pl.col("ActivityDate").str.strptime(pl.datatypes.Date, "%Y-%m-%dT%H:%M:%S%.f%z", strict=False))
        lf = lf.with_columns(pl.col("CustomerReplyDate").str.strptime(pl.datatypes.Date, "%Y-%m-%dT%H:%M:%S%.f%z", strict=False))
        
        # Remove erroneous data, e.g. future dates
        yesterday = datetime.now() - timedelta(1)
        lf = lf.filter(pl.col("FirstResponseDate") < pl.lit(yesterday.date()))
        lf.drop("ActivityDate")

        # TODO: Below not idea for runtime. Fix.
        for v in IdDict.values():
            for subk, subv in v.items():
                lf = lf.with_columns(pl.when(pl.col("ActivityTypeId") == subk).then(1).otherwise(0).alias(subv))
    
        lf = lf.groupby("Origin").agg([
            pl.first("FirstResponseDate"),
            pl.first("CustomerReplyDate"),
            pl.first("CustomerReplyType"),
            pl.sum(IdDict["Store Contact Method"][3]),
            pl.sum(IdDict["Store Contact Method"][4]),
            pl.sum(IdDict["Store Contact Method"][35])
            
        ])

        lf = lf.join(lf_response, on="Origin", how="left")
        lf = lf.join(lf_CustFirst, on="Origin", how="left")
        
        lf = lf.select([
            "Origin",
            "CustContactB4StoreDate",
            "CustContactMethodIfB4Store",
            "#CustContactB4Store",
            "FirstResponseDate",
            "FirstResponseType",
            "ResponseDateBeforeReply",
            "ResponseTypeBeforeReply",
            "CustomerReplyDate",
            "CustomerReplyType",
            "Store Method A",
            "Store Method B",
            "Store Method C"
        ])
        
        # Actualize the dataframe
        df = lf.collect()
        
        df.write_csv(f"response_counts_{suffix}.csv")
        toc = timeit.default_timer()
        time = toc - tic
        print(f"Run Time: {time}s\n\n")
    
def main():
    dir_path = input("Provide path to directory containing csvs: ")
    responseCount_to_csv(dir_path)
    
if __name__ == '__main__':
    main()
