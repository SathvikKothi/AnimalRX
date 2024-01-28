import json
import os
import requests
import pandas as pd

df = pd.read_json("https://api.fda.gov/download.json")
df.index
get_data = df[~df.index.isin(["disclaimer", "terms", "license", "last_updated"])]

for i in get_data["results"]:
    files = []
    import json
    import pandas as pd
    import os

    resultsdata = json.dumps(i)


def resultsdata(get_data):
    for i in get_data["results"]:
        num = 0
        import json
        import pandas as pd
        import os

        resultsdata = json.dumps(i)
        data = json.loads(resultsdata)
        # print(data)
        for key, value in data.items():
            partitions = value["partitions"]
            for partition in partitions:
                file_url = partition["file"]
                response = requests.get(file_url)
                retry_count = 0
                if response.status_code == 200 or retry_count > 2:
                    try:
                        file_content = response.content
                        # Create the folder if it doesn't exist
                        folder_name = key
                        folder_name = file_url.replace(
                            "https://download.open.fda.gov/", "./"
                        ).replace(file_url.split("/")[-1], "")
                        os.makedirs(folder_name, exist_ok=True)

                        # Extract the filename from the file URL
                        file_name = file_url.split("/")[-1]
                        # Construct the full file path
                        file_path = os.path.join(folder_name, file_name)
                        # Save the file
                        with open(file_path, "wb") as f:
                            f.write(file_content)
                        print(f"File downloaded: {file_path}")
                    except Exception as er:
                        print(er)
                        sleep(20)
                        continue
                else:
                    print(f"Failed to retrieve file: {file_url}")
                    sleep(20)
                    retry_count = retry_count + 1
                    continue


# Call the function
resultsdata(get_data)


## Another download portal for metadata and other information.
##https://www.fda.gov/medical-devices/device-registration-and-listing/establishment-registration-and-medical-device-listing-files-download
