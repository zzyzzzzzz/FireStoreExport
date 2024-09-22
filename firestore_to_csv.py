import firebase_admin
from firebase_admin import credentials, firestore
import csv
from datetime import datetime

cred_path = 'smarter-time-use-surveys-firebase-adminsdk-g1olm-65757b9d37.json'

def export_firestore_to_csv(collection_path, output_file):
    print("Data export starts")

    # Initialize Firestore client
    cred = credentials.Certificate(cred_path)
    firebase_admin.initialize_app(cred)
    db = firestore.client()

    with open(output_file, 'w', newline='', encoding='utf-8') as csvfile:
        csvwriter = csv.writer(csvfile)
        # Write CSV header
        header = [
            "Participant #", "Report #", "Activity #", "Activity Code",
            "Start Time", "End Time",
            "Enjoyment Rating", "Other People", "Secondary Activity Code",
            "Response Start Time", "Response Submit Time",
            "ResponseStartTimeHour", "ResponseStartTimeMinute", "ResponseStartTimeSecond",
            "ResponseSubmitTimeHour", "ResponseSubmitTimeMinute", "ResponseSubmitTimeSecond"
        ]
        csvwriter.writerow(header)

        # Iterate through all documents in the specified collection
        for doc_ref in db.collection(collection_path).stream():
            participant_id = doc_ref.id  # Participant ID as Participant #

            # Check if 'responses' subcollection exists
            responses_docs = db.collection(collection_path).document(participant_id).collection('responses').limit(1).stream()
            has_responses = list(responses_docs)  # Check if there are any responses

            if not has_responses:
                # Write a single entry with empty response fields
                csvwriter.writerow([participant_id, "", "", "", "", "", "", "", "", "", "", "", "", "", "", ""])
                continue  # Skip to the next participant

            # Responses exist, iterate through them
            responses_docs = db.collection(collection_path).document(participant_id).collection('responses').stream()
            responses_list = sorted(responses_docs, key=lambda x: x.id)

            for response_index, response_doc_ref in enumerate(responses_list, start=0):
                response_data = response_doc_ref.to_dict()
                response_start_time = response_data.get('ResponseStartTime', None)
                response_submit_time = response_data.get('ResponseSubmitTime', None)

                def extract_time_components(time_value):
                    if time_value:
                        if isinstance(time_value, datetime):
                            return time_value.hour, time_value.minute, time_value.second
                        else:
                            print(f"Unexpected type for time value: {type(time_value)}")
                    return "", "", ""

                response_start_hour, response_start_minute, response_start_second = extract_time_components(response_start_time)
                response_submit_hour, response_submit_minute, response_submit_second = extract_time_components(response_submit_time)

                # Get the activities subcollection for each response
                activities_docs = db.collection(collection_path).document(participant_id).collection(
                    'responses').document(response_doc_ref.id).collection('activities').stream()

                activities_list = list(activities_docs)

                # Write header for each response
                csvwriter.writerow([participant_id, response_index, "", "", "", "", "", "", "", response_start_time, response_submit_time,
                                    response_start_hour, response_start_minute, response_start_second,
                                    response_submit_hour, response_submit_minute, response_submit_second])

                if not activities_list:
                    continue  # Skip to the next response

                # Iterate through activities and write entries
                for activity_index, activity_doc_ref in enumerate(activities_list, start=1):
                    activity_data = activity_doc_ref.to_dict()
                    activity_number = activity_index
                    activity_code = activity_data.get('activityCode', '')
                    start_time = activity_data.get('startTime', '')
                    end_time = activity_data.get('endTime', '')
                    enjoyment_rating = activity_data.get('enjoymentRating', '')
                    other_people = activity_data.get('otherPeople', '')
                    secondary_activity_code = activity_data.get('secondaryActivityCode', '')

                    # Write to CSV, ensuring empty values are represented as well
                    csvwriter.writerow([participant_id, response_index, activity_number, activity_code,
                                        start_time, end_time, enjoyment_rating, other_people, secondary_activity_code,
                                        response_start_time, response_submit_time,
                                        response_start_hour, response_start_minute, response_start_second,
                                        response_submit_hour, response_submit_minute, response_submit_second])

    print("Data export complete.")

# Call the function to export data
export_firestore_to_csv('study1', 'output.csv')
