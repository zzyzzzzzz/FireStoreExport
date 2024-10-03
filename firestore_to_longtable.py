import firebase_admin
from firebase_admin import credentials, firestore
import csv

cred_path = 'smarter-time-use-surveys-firebase-adminsdk-g1olm-65757b9d37.json'

# Dictionary to map activity codes to names
activity_code_map = {
    1: "Paid work",
    2: "Home production",
    3: "Unpaid Housework",
    31: "Cooking",
    32: "Cleaning and Tidying",
    33: "Maintenance and Repairs",
    34: "Washing and Repairing Clothing",
    35: "Household Planning and Finances",
    36: "Pet Care",
    37: "Shopping",
    38: "Travelling for Household Needs",
    39: "Other Housework",
    4: "Unpaid Child and Family Care",
    41: "Care of Infants",
    42: "Care of Other Children",
    43: "Caring for Elderly or Disabled Adults",
    44: "Caring for Other Adult Household Members",
    45: "Travelling for Care Needs",
    49: "Other Care Work",
    5: "Unpaid Volunteer and Trainee Work",
    51: "Volunteering for Other Households",
    52: "Volunteering for Community",
    53: "Unpaid trainee work",
    54: "Travelling to Volunteer or Trainee Work",
    59: "Other Unpaid Volunteer or Trainee Work",
    6: "Learning",
    7: "Socialising and Community Activities",
    8: "Sports and Recreation",
    9: "Personal Washing and Grooming"
}


def export_firestore_to_longtable(collection_path, output_file):
    print("Data export starts")

    # Initialize Firestore client
    cred = credentials.Certificate(cred_path)
    firebase_admin.initialize_app(cred)
    db = firestore.client()

    with open(output_file, 'w', newline='', encoding='utf-8') as csvfile:
        csvwriter = csv.writer(csvfile)

        # Define CSV header
        header = [
                     "Household ID", "Person ID", "Date", "Survey Number",
                     "Start Date Time stamp", "year", "Month", "Day", "hour", "minute",
                     "Submit Date Time stamp", "year", "Month", "Day", "hour", "minute",
                     "delay between ping and filling in (minutes)", "time taken to fill in"
                 ] + [f"Time Use: {activity_code_map[code]}" for code in sorted(activity_code_map.keys())]

        csvwriter.writerow(header)

        # Iterate through all documents in the specified collection
        for doc_ref in db.collection(collection_path).stream():
            household_id = doc_ref.id  # Household ID as Person ID

            responses_docs = db.collection(collection_path).document(household_id).collection('responses').stream()
            responses_list = sorted(responses_docs, key=lambda x: x.id)

            for response_index, response_doc_ref in enumerate(responses_list, start=1):
                response_data = response_doc_ref.to_dict()
                response_start_time = response_data.get('ResponseStartTime', None)
                response_submit_time = response_data.get('ResponseSubmitTime', None)

                # Extract date and time components for start and submit times
                if response_start_time:
                    start_year = response_start_time.year
                    start_month = response_start_time.month
                    start_day = response_start_time.day
                    start_hour = response_start_time.hour
                    start_minute = response_start_time.minute
                else:
                    start_year = start_month = start_day = start_hour = start_minute = ""

                if response_submit_time:
                    submit_year = response_submit_time.year
                    submit_month = response_submit_time.month
                    submit_day = response_submit_time.day
                    submit_hour = response_submit_time.hour
                    submit_minute = response_submit_time.minute
                else:
                    submit_year = submit_month = submit_day = submit_hour = submit_minute = ""

                # Calculate the delay between start and submit times (in minutes)
                if response_start_time and response_submit_time:
                    delay_minutes = int((response_submit_time - response_start_time).total_seconds() / 60)
                else:
                    delay_minutes = ""

                # Assuming the time taken to fill in is the same as delay
                time_taken_to_fill_in = delay_minutes

                # Get the activities subcollection for each response
                activities_docs = db.collection(collection_path).document(
                    household_id).collection('responses').document(response_doc_ref.id).collection(
                    'activities').stream()

                # Create a list to track time use for each activity
                time_use = {code: "" for code in sorted(activity_code_map.keys())}

                for activity_doc_ref in activities_docs:
                    activity_data = activity_doc_ref.to_dict()
                    activity_code = activity_data.get('activityCode', None)
                    start_time = activity_data.get('startTime', None)
                    end_time = activity_data.get('endTime', None)

                    # Calculate the time spent on each activity (in minutes)
                    if start_time is not None and end_time is not None and activity_code in time_use:
                        time_spent = end_time - start_time  # End time - Start time (in minutes)
                        time_use[activity_code] = time_spent

                # Fill in the row data
                row = [
                          household_id, household_id, response_start_time.date() if response_start_time else "",
                          response_index,
                          response_start_time, start_year, start_month, start_day, start_hour, start_minute,
                          response_submit_time, submit_year, submit_month, submit_day, submit_hour, submit_minute,
                          delay_minutes, time_taken_to_fill_in  # Using delay_minutes for both columns
                      ] + [time_use[code] for code in sorted(activity_code_map.keys())]

                csvwriter.writerow(row)

    print("Data export complete.")


# Call the function to export data
export_firestore_to_longtable('study1', 'output_longtable.csv')
