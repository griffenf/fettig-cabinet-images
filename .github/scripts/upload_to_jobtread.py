#!/usr/bin/env python3
"""
Processes pending-uploads/*.json queue files and uploads cabinet images to JobTread.
Each JSON file contains the cost item ID and the path to the image in the repo.
After uploading, queue files are deleted and the action commits the cleanup.
"""

import os
import json
import glob
import requests

GRANT_KEY = os.environ["JOBTREAD_GRANT_KEY"]
ORG_ID = os.environ["JOBTREAD_ORG_ID"]
PAVE_URL = "https://api.jobtread.com/pave"


def jobtread_query(query):
    response = requests.post(
        PAVE_URL,
        json={"query": {"$": {"grantKey": GRANT_KEY}, **query}},
        headers={"Content-Type": "application/json"},
        timeout=30,
    )
    response.raise_for_status()
    return response.json()


def upload_image_to_jobtread(image_path, filename):
    """Creates an upload request, uploads image to Google Storage, returns upload request ID."""
    with open(image_path, "rb") as f:
        image_data = f.read()

    file_size = len(image_data)

    # Determine content type
    ext = filename.lower().split(".")[-1]
    content_type = "image/png" if ext == "png" else "image/jpeg"

    # Step 1: Create upload request
    result = jobtread_query({
        "createUploadRequest": {
            "$": {
                "organizationId": ORG_ID,
                "size": file_size,
                "type": {"fromName": filename},
            },
            "createdUploadRequest": {
                "id": {},
                "url": {},
                "method": {},
                "headers": {},
            },
        }
    })

    upload_req = result["createUploadRequest"]["createdUploadRequest"]
    upload_url = upload_req["url"]
    upload_req_id = upload_req["id"]
    headers = upload_req.get("headers", {})
    headers["content-type"] = content_type

    # Step 2: Upload to Google Storage
    upload_resp = requests.put(upload_url, data=image_data, headers=headers, timeout=60)
    if upload_resp.status_code != 200:
        raise Exception(f"Google Storage upload failed: {upload_resp.status_code} {upload_resp.text}")

    print(f"  ✓ Uploaded to Google Storage (request ID: {upload_req_id})")
    return upload_req_id


def attach_to_cost_item(cost_item_id, upload_req_id, filename):
    """Attaches the uploaded file to a JobTread cost item."""
    result = jobtread_query({
        "updateCostItem": {
            "$": {
                "id": cost_item_id,
                "files": [
                    {
                        "_type": "new",
                        "name": filename,
                        "uploadRequestId": upload_req_id,
                    }
                ],
            },
            "costItem": {
                "$": {"id": cost_item_id},
                "id": {},
                "name": {},
            },
        }
    })
    name = result["updateCostItem"]["costItem"]["name"]
    print(f"  ✓ Attached to cost item: {name}")


def main():
    queue_files = sorted(glob.glob("pending-uploads/*.json"))

    if not queue_files:
        print("No pending uploads found.")
        return

    print(f"Found {len(queue_files)} pending upload(s).\n")
    success_count = 0
    fail_count = 0

    for queue_file in queue_files:
        with open(queue_file) as f:
            job = json.load(f)

        cost_item_id = job["costItemId"]
        image_path = job["imagePath"]
        filename = os.path.basename(image_path)

        print(f"Processing: {filename} → cost item {cost_item_id}")

        try:
            upload_req_id = upload_image_to_jobtread(image_path, filename)
            attach_to_cost_item(cost_item_id, upload_req_id, filename)
            os.remove(queue_file)
            print(f"  ✓ Queue file removed: {queue_file}\n")
            success_count += 1
        except Exception as e:
            print(f"  ✗ Error: {e}\n")
            fail_count += 1

    print(f"Done. {success_count} succeeded, {fail_count} failed.")


if __name__ == "__main__":
    main()
