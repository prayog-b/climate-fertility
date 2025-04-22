import cdsapi
from queue import Queue, Empty
import threading

coordinates = {
    "Africa": {"north":37.8, "south":-39.3, "east":55.2, "west":-25.5}
}
folder_path = "D:/NUS Dropbox/Prayog Bhattarai/Climate_Change_and_Fertility_in_SSA/data/source/era"

def worker(api_key, country, queue):
    coords = coordinates[country]
    c = cdsapi.Client(url="https://cds.climate.copernicus.eu/api", key = api_key)
    dataset = "reanalysis-era5-single-levels"

    while True:
        try:
            year = queue.get_nowait()
        except Empty:
            break
                
        try:
            print(f"Downloading {country} {year} with key {api_key[-4:]}")
            request = {
                "product_type": "reanalysis",
                "variable": [
                    "2m_temperature",
                    "total_precipitation"
                ],
                "year": [str(year)],
                "month": ["01", "02", "03", "04", "05", "06", "07",
                         "08", "09", "10", "11", "12"],
                "day": ["01", "02", "03", "04", "05", "06", "07",
                       "08", "09", "10", "11", "12", "13", "14",
                       "15", "16", "17", "18", "19", "20", "21",
                       "22", "23", "24", "25", "26", "27", "28",
                       "29", "30", "31"],
                "time": ["00:00", "01:00", "02:00", "03:00",
                        "04:00", "05:00", "06:00", "07:00",
                        "08:00", "09:00", "10:00", "11:00",
                        "12:00", "13:00", "14:00", "15:00",
                        "16:00", "17:00", "18:00", "19:00",
                        "20:00", "21:00", "22:00", "23:00"],
                "format": "netcdf",
                "area": [coords["north"], coords["west"], coords["south"], coords["east"]]
            }
            c.retrieve(dataset, request, f"{folder_path}/{country}_{year}.nc")
        except Exception as e:
            print(f"Failed to download {year}: {e}")
            queue.put(year)
        finally:
            queue.task_done()
        
def main():
    country = "Africa"
    years = list(range(1940, 2020))
    queue = Queue()
    for year in years:
        queue.put(year)
    api_keys = [
        "d051299d-2f34-4039-8ad9-9d210502f5ca",
        "503415af-583f-46ea-8b19-b8b0a7ac6f1c",
        "3ebf6659-4d63-40ee-bbe9-8417d4e22e6a",
        "01bef106-fa50-4249-a6d1-c551a74857a4"
    ]

    threads = []
    for key in api_keys:
        thread = threading.Thread(target = worker, args = (key, country, queue))
        thread.start()
        threads.append(thread)

    queue.join()

    for thread in threads:
        thread.join()

    print("All downloads completed.")

if __name__ == "__main__":
    main()