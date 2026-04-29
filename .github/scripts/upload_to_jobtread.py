#!/usr/bin/env python3
import os, json, glob, requests

GRANT_KEY = os.environ["JOBTREAD_GRANT_KEY"]
ORG_ID = os.environ["JOBTREAD_ORG_ID"]
PAVE_URL = "https://api.jobtread.com/pave"

def jobtread_query(query):
    payload = {"query": {"$": {"grantKey": GRANT_KEY}, **query}}
    response = requests.post(PAVE_URL, json=payload, headers={"Content-Type": "application/json"}, timeout=30)
    print(f"  HTTP {response.status_code}: {response.text[:800]}")
    response.raise_for_status()
    result = response.json()
    if "errors" in result:
        raise Exception(f"API error: {result['errors']}")
    return result

def upload_one_image(image_path, filename):
    with open(image_path, "rb") as f:
        image_data = f.read()
    file_size = len(image_data)

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
    headers["content-type"] = "image/png"
    print(f"  Upload request ID: {upload_req_id}")

    upload_resp = requests.put(upload_url, data=image_data, headers=headers, timeout=60)
    print(f"  GCS response: {upload_resp.status_code}")
    if upload_resp.status_code not in (200, 204):
        raise Exception(f"GCS upload failed: {upload_resp.status_code}")
    return upload_req_id

def get_existing_files(target_type, target_id):
    if target_type == "costGroup":
        result = jobtread_query({"costGroup": {"$": {"id": target_id}, "files": {"nodes": {"id": {}, "name": {}}}}})
        return result["costGroup"]["files"]["nodes"]
    else:
        result = jobtread_query({"costItem": {"$": {"id": target_id}, "files": {"nodes": {"id": {}, "name": {}}}}})
        return result["costItem"]["files"]["nodes"]

def main():
    queue_files = sorted(glob.glob("pending-uploads/*.json"))
    if not queue_files:
        print("No pending uploads found.")
        return
    print(f"Found {len(queue_files)} upload(s).")

    for queue_file in queue_files:
        with open(queue_file) as f:
            job = json.load(f)

        target_type = job.get("targetType", "costItem")
        target_id = job.get("targetId") or job.get("costItemId")
        image_paths = job.get("imagePaths") or [job["imagePath"]]
        print(f"\nProcessing {len(image_paths)} image(s) -> {target_type} {target_id}")

        try:
            # Fetch existing files and preserve them using correct _type
            existing_nodes = get_existing_files(target_type, target_id)
            print(f"  Preserving {len(existing_nodes)} existing file(s): {[f['name'] for f in existing_nodes]}")
            existing_payload = [{"_type": "lineItemFile", "id": f["id"], "name": f["name"]} for f in existing_nodes]

            # Upload new files
            new_payload = []
            for image_path in image_paths:
                filename = os.path.basename(image_path)
                print(f"  Uploading: {filename}")
                upload_req_id = upload_one_image(image_path, filename)
                new_payload.append({"name": filename, "uploadRequestId": upload_req_id})
                print(f"  ✓ {filename} uploaded to GCS")

            files_payload = existing_payload + new_payload
            print(f"  Total files in payload: {len(files_payload)}")

            if target_type == "costGroup":
                result = jobtread_query({
                    "updateCostGroup": {
                        "$": {"id": target_id, "files": files_payload},
                        "costGroup": {"$": {"id": target_id}, "id": {}, "name": {}}
                    }
                })
                name = result["updateCostGroup"]["costGroup"]["name"]
                print(f"  ✓ Attached to cost group '{name}'")
            else:
                result = jobtread_query({
                    "updateCostItem": {
                        "$": {"id": target_id, "files": files_payload},
                        "costItem": {
                            "$": {"id": target_id},
                            "id": {}, "name": {},
                            "files": {"nodes": {"id": {}, "name": {}}}
                        }
                    }
                })
                item = result["updateCostItem"]["costItem"]
                files = item.get("files", {}).get("nodes", [])
                print(f"  ✓ Attached! '{item['name']}' now has {len(files)} file(s)")

            os.remove(queue_file)
            print(f"  ✓ Queue file removed")
        except Exception as e:
            print(f"  ✗ Error: {e}")

    print("\nDone!")

if __name__ == "__main__":
    main()
