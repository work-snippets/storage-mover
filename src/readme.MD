# Storage Mover: S3 to Azure Blob

## Get Azure Storage Connection String

```sh
az storage account show-connection-string --name encorastorage --resource-group encora
```

## Sample IAM Role for AWS

1. **Create a trust policy file (`trust-policy.json`):**

   ```json
   {
     "Version": "2012-10-17",
     "Statement": [
       {
         "Effect": "Allow",
         "Principal": { "AWS": "*" },
         "Action": "sts:AssumeRole"
       }
     ]
   }
   ```

2. **Create the role:**

   ```sh
   aws iam create-role \
     --role-name S3ToAzureMoverRole \
     --assume-role-policy-document file://trust-policy.json
   ```

3. **Attach S3 read or full access policy:**

   ```sh
   aws iam attach-role-policy \
     --role-name S3ToAzureMoverRole \
     --policy-arn arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess
   ```

   _Or use `AmazonS3FullAccess` if you need write/delete permissions._

4. **Get the role ARN:**
   ```sh
   aws iam get-role --role-name S3ToAzureMoverRole
   ```
   Use the `"Arn"` value in your `.env` as `AWS_ROLE_ARN`.

---

## Environment Variables

Copy `.env.sample` to `.env` and fill in your values.  
**Do not commit your real `.env` file.**

---

## .gitignore

Make sure your `.gitignore` includes:

```
.env
.env.*
*.env
```

---

## Usage

Run the mover script after setting up your `.env`:

```sh
python mover.py
```
