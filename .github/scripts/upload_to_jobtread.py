#!/usr/bin/env python3
import os, json, glob, requests

GRANT_KEY = os.environ["JOBTREAD_GRANT_KEY"]
ORG_ID = os.environ["JOBTREAD_ORG_ID"]
PAVE_URL = "https://api.jobtread.com/pave"

def jobtread_query(query):
    payload = {"query": {"$": {"grantKey": GRANT_KEY}, **query}}
    response = requests.post(PAVE_URL, json=payload, headers={"Content-Type": "application/json"}, timeout=30)
    print(f"  HTTP {response.status_code}: {response.text[:500]}")
    response.raise_for_status()
    result = response.json()
    if "errors" in result:
        raise Exception(f"API error: {result['errors']}")
    return result

def main():
    queue_files = sorted(glob.glob("pending-uploads/*.json"))
    if not queue_files:
        print("No pending uploads found.")
        return
    print(f"Found {len(queue_files)} upload(s).")

    for queue_file in queue_files:
        with open(queue_file) as f:
            job = json.load(f)
        cost_item_id = job["costItemId"]
        image_path = job["imagePath"]
        filename = os.path.basename(image_path)
        print(f"\nProcessing: {filename} -> {cost_item_id}")

        with open(image_path, "rb") as f:
            image_data = f.read()
        file_size = len(image_data)
        ext = filename.lower().split(".")[-1]
        content_type = "image/png" if ext == "png" else "image/jpeg"
        print(f"  Size: {file_size} bytes, type: {content_type}")

        # Step 1: Create upload request
        result = jobtread_query({
            "createUploadRequest": {
                "$": {"organizationId": ORG_ID, "size": file_size, "type": {"fromName": filename}},
                "createdUploadRequest": {"id": {}, "url": {}, "method": {}, "headers": {}}
            }
        })
        upload_req = result["createUploadRequest"]["createdUploadRequest"]
        upload_url = upload_req["url"]
        upload_req_id = upload_req["id"]
        headers = dict(upload_req.get("headers") or {})
        headers["content-type"] = content_type
        print(f"  Upload request ID: {upload_req_id}")

        # Step 2: Upload to Google Storage
        upload_resp = requests.put(upload_url, data=image_data, headers=headers, timeout=60)
        print(f"  GCS response: {upload_resp.status_code} {upload_resp.text[:200]}")
        if upload_resp.status_code not in (200, 204):
            raise Exception(f"GCS upload failed: {upload_resp.status_code}")
        print("  ✓ Uploaded to GCS")

        # Step 3: Attach to cost item (no _type for new files)
        result = jobtread_query({
            "updateCostItem": {
                "$": {
                    "id": cost_item_id,
                    "files": [{"name": filename, "uploadRequestId": upload_req_id}]
                },
                "costItem": {"$": {"id": cost_item_id}, "id": {}, "name": {}, "files": {"nodes": {"id": {}, "name": {}}}}
            }
        })
        item = result["updateCostItem"]["costItem"]
        files = item.get("files", {}).get("nodes", [])
        print(f"  ✓ Attached! Cost item now has {len(files)} file(s): {[f['name'] for f in files]}")
        os.remove(queue_file)
        print(f"  ✓ Queue file removed")

    print("\nDone!")

if __name__ == "__main__":
    main()
