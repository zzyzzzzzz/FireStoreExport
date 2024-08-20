import firebase_admin
from firebase_admin import credentials, firestore
import csv

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
            # "Registration Time",  # Registration Time removed
            "Response Start Time", "Response Submit Time", "Enjoyment"
        ]
        csvwriter.writerow(header)

        # Iterate through all documents in the specified collection
        for doc_ref in db.collection(collection_path).stream():
            doc = doc_ref.to_dict()
            participant_id = doc_ref.id  # Participant ID as Participant #
            # registration_time = doc.get('Registration time', '')  # Registration time removed

            # Check if 'responses' subcollection exists
            responses_docs = db.collection(collection_path).document(participant_id).collection('responses').limit(1).stream()
            has_responses = list(responses_docs)  # Get at least one document to check existence

            if not has_responses:
                # If there are no responses, write a single entry with empty response fields
                csvwriter.writerow([
                    participant_id, "", "", "", "", "",
                    # registration_time,  # Registration Time removed
                    "", "", ""
                ])
                continue  # Skip to the next participant

            # Responses exist, iterate through them
            responses_docs = db.collection(collection_path).document(participant_id).collection('responses').stream()
            responses_list = sorted(responses_docs, key=lambda x: x.id)

            for response_index, response_doc_ref in enumerate(responses_list, start=0):  # Start index at 0
                response_data = response_doc_ref.to_dict()
                response_start_time = response_data.get('ResponseStartTime', '')
                response_submit_time = response_data.get('ResponseSubmitTime', '')

                # Get the activities subcollection for each response
                activities_docs = db.collection(collection_path).document(participant_id).collection(
                    'responses').document(response_doc_ref.id).collection('activities').stream()

                # Convert activity documents to a list
                activities_list = list(activities_docs)

                # Write header for each response
                csvwriter.writerow([
                    participant_id, response_index, "", "", "", "",
                    # registration_time,  # Registration Time removed
                    response_start_time, response_submit_time, ""
                ])

                # If there are no activities, leave the activity fields empty
                if not activities_list:
                    continue  # Skip to the next response

                # Iterate through activities and write entries
                for activity_index, activity_doc_ref in enumerate(activities_list, start=1):
                    activity_data = activity_doc_ref.to_dict()
                    activity_number = activity_index  # Use a separate index for activities
                    activity_code = activity_data.get('activityCode', '')
                    start_time = activity_data.get('startTime', '')
                    end_time = activity_data.get('endTime', '')

                    # Write to CSV
                    csvwriter.writerow([
                        participant_id, response_index, activity_number, activity_code,
                        start_time, end_time,
                        # registration_time,  # Registration Time removed
                        response_start_time, response_submit_time, ""
                    ])

    print("Data export complete.")

# Call the function to export data
export_firestore_to_csv('study1', 'output.csv')
