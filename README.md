# integ2json

Converts Intersystems Cache files from:
```
Cache Database Integrity Check on 01/22/2019 at 19:00:01
System: HOSTNAME  Configuration: CACHE
Cache for OS (OS DETAILS) Version


---Directory database_dir---

Global: GLOBAL
 Top/Bottom Pnt Level: # of blocks=1       8kb (4% full)
 Data Level:           # of blocks=18      144kb (72% full)
 Total:                # of blocks=19      152kb (69% full)
 Elapsed Time = 0.0 seconds, Completed 01/22/2019 19:00:01

Global: GLOBAL2
 Top/Bottom Pnt Level: # of blocks=1      8kb (0% full)
 Data Level:           # of blocks=1      8kb (7% full)
 Total:                # of blocks=2      16kb (3% full)
 Elapsed Time = 0.0 seconds, Completed 01/22/2019 19:00:01
```

into this:

```
{
    "INTEG_ALL_20190122-190001.txt": {
        "databases": {
            "database_dir": {
                "globals": {
                    "GLOBAL1": {
                        "Data": {
                            "Data_Level": {
                                "Blocks": 18, 
                                "Percent_Full": 72, 
                                "Size_KB": 144
                            }, 
                            "Top_Bottom_Pnt_Level": {
                                "Blocks": 1, 
                                "Percent_Full": 4, 
                                "Size_KB": 8
                            }, 
                            "Total": {
                                "Blocks": 19, 
                                "Percent_Full": 69, 
                                "Size_KB": 152
                            }
                        }, 
                        "Time": {
                            "Elapsed_Seconds": 0.0, 
                            "End_Date": "01/22/2019", 
                            "End_Time": "19:00:01"
                        }
                    }, 
                    "GLOBAL2": {
                        "Data": {
                            "Data_Level": {
                                "Blocks": 1, 
                                "Percent_Full": 7, 
                                "Size_KB": 8
                            }, 
                            "Top_Bottom_Pnt_Level": {
                                "Blocks": 1, 
                                "Percent_Full": 0, 
                                "Size_KB": 8
                            }, 
                            "Total": {
                                "Blocks": 2, 
                                "Percent_Full": 3, 
                                "Size_KB": 16
                            }
                        }, 
                        "Time": {
                            "Elapsed_Seconds": 0.0, 
                            "End_Date": "01/22/2019", 
                            "End_Time": "19:00:01"
                        }
                    }
                }
            }
        }
    }
}
```
