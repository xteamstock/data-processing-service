Authentication for push subscriptions

bookmark_border
If a push subscription uses authentication, the Pub/Sub service signs a JWT and sends the JWT in the authorization header of the push request. The JWT includes claims and a signature.

Subscribers can validate the JWT and verify the following:

The claims are accurate.
The Pub/Sub service signed the claims.
If subscribers use a firewall, they can't receive push requests. To receive push requests, you must turn off the firewall and verify the JWT.

Before you begin

Learn about subscriptions.
Understand how push subscriptions work.
Create a push subscription.
JWT format

The JWT is an OpenIDConnect JWT that consists of a header, claim set, and signature. The Pub/Sub service encodes the JWT as a base64 string with period delimiters.

For example, the following authorization header includes an encoded JWT:


"Authorization" : "Bearer
eyJhbGciOiJSUzI1NiIsImtpZCI6IjdkNjgwZDhjNzBkNDRlOTQ3MTMzY2JkNDk5ZWJjMWE2MWMzZDVh
YmMiLCJ0eXAiOiJKV1QifQ.eyJhdWQiOiJodHRwczovL2V4YW1wbGUuY29tIiwiYXpwIjoiMTEzNzc0M
jY0NDYzMDM4MzIxOTY0IiwiZW1haWwiOiJnYWUtZ2NwQGFwcHNwb3QuZ3NlcnZpY2VhY2NvdW50LmNvb
SIsImVtYWlsX3ZlcmlmaWVkIjp0cnVlLCJleHAiOjE1NTAxODU5MzUsImlhdCI6MTU1MDE4MjMzNSwia
XNzIjoiaHR0cHM6Ly9hY2NvdW50cy5nb29nbGUuY29tIiwic3ViIjoiMTEzNzc0MjY0NDYzMDM4MzIxO
TY0In0.QVjyqpmadTyDZmlX2u3jWd1kJ68YkdwsRZDo-QxSPbxjug4ucLBwAs2QePrcgZ6hhkvdc4UHY
4YF3fz9g7XHULNVIzX5xh02qXEH8dK6PgGndIWcZQzjSYfgO-q-R2oo2hNM5HBBsQN4ARtGK_acG-NGG
WM3CQfahbEjZPAJe_B8M7HfIu_G5jOLZCw2EUcGo8BvEwGcLWB2WqEgRM0-xt5-UPzoa3-FpSPG7DHk7
z9zRUeq6eB__ldb-2o4RciJmjVwHgnYqn3VvlX9oVKEgXpNFhKuYA-mWh5o7BCwhujSMmFoBOh6mbIXF
cyf5UiVqKjpqEbqPGo_AvKvIQ9VTQ" 
The header and claim set are JSON strings. Once decoded, they take the following form:


{"alg":"RS256","kid":"7d680d8c70d44e947133cbd499ebc1a61c3d5abc","typ":"JWT"}

{
   "aud":"https://example.com",
   "azp":"113774264463038321964",
   "email":"gae-gcp@appspot.gserviceaccount.com",
   "sub":"113774264463038321964",
   "email_verified":true,
   "exp":1550185935,
   "iat":1550182335,
   "iss":"https://accounts.google.com"
  }
The tokens attached to requests sent to push endpoints may be up to an hour old.

Configure Pub/Sub for push authentication

The following example shows how to set the push auth service account to a service account of your choice and how to grant the iam.serviceAccountTokenCreator role to the service-{PROJECT_NUMBER}@gcp-sa-pubsub.iam.gserviceaccount.com service agent.

Console
gcloud
Go to the Pub/Sub Subscriptions page.

Go to the Subscriptions page
Click Create subscription.
In the Subscription ID field, enter a name.
Select a topic.
Select Push as the Delivery type.
Enter an endpoint URL.
Check Enable authentication.
Select a service account.
Ensure that the service agent service-{PROJECT_NUMBER}@gcp-sa-pubsub.iam.gserviceaccount.com has the iam.serviceAccountTokenCreator role in your project's IAM dashboard. If the service account has not been granted the role, then click Grant in the IAM dashboard to do so.
Optional: Enter an audience.
Click Create.
When you're enabling authentication for a push subscription, you might encounter a permission denied or not authorized error. To resolve this issue, give the principal initiating the creation or update of the subscription the iam.serviceAccounts.actAs permission on the service account. For more information, see Authentication in "Create push subscriptions."

If you use an authenticated push subscription with an App Engine application that is secured with Identity-Aware Proxy, you must provide the IAP Client ID as your push auth token audience. To enable IAP on your App Engine application, see Enabling IAP. To find the IAP client ID, look for IAP-App-Engine-app Client ID on the Credentials page.

Claims

The JWT can be used to validate that the claims -- including email and aud claims -- are signed by Google. For more information about how Google's OAuth 2.0 APIs can be used for both authentication and authorization, see OpenID Connect.

There are two mechanisms that make these claims meaningful. First, Pub/Sub requires that the user or service account making the CreateSubscription, UpdateSubscription, or ModifyPushConfig call to have a role with the iam.serviceAccounts.actAs permission on the push auth service account. An example of such a role is the roles/iam.serviceAccountUser role.

Second, access to the certificates used to sign the tokens is tightly controlled. To create the token, Pub/Sub must call an internal Google service using a separate signing service account identity, which is the service agent service-${PROJECT_NUMBER}@gcp-sa-pubsub.iam.gserviceaccount.com. This signing service account must have the iam.serviceAccounts.getOpenIdToken permission or a Service Account Token Creator role (roles/iam.serviceAccountTokenCreator) on the push auth service account (or on any ancestor resource, such as the project, of the push auth service account).

Validate tokens

Validating tokens sent by Pub/Sub to the push endpoint involves:

Checking the token integrity by using signature validation.
Ensuring that the email and audience claims in the token match the values set in the push subscription configuration.
The following example illustrates how to authenticate a push request to an App Engine application not secured with Identity-Aware Proxy. If your App Engine application is secured with IAP, the HTTP request header that contains the IAP JWT is x-goog-iap-jwt-assertion and must be validated accordingly.

protocol
C#
Go
Java
Node.js
Python
Ruby



@app.route("/push-handlers/receive_messages", methods=["POST"])
def receive_messages_handler():
    # Verify that the request originates from the application.
    if request.args.get("token", "") != current_app.config["PUBSUB_VERIFICATION_TOKEN"]:
        return "Invalid request", 400

    # Verify that the push request originates from Cloud Pub/Sub.
    try:
        # Get the Cloud Pub/Sub-generated JWT in the "Authorization" header.
        bearer_token = request.headers.get("Authorization")
        token = bearer_token.split(" ")[1]
        TOKENS.append(token)

        # Verify and decode the JWT. `verify_oauth2_token` verifies
        # the JWT signature, the `aud` claim, and the `exp` claim.
        # Note: For high volume push requests, it would save some network
        # overhead if you verify the tokens offline by downloading Google's
        # Public Cert and decode them using the `google.auth.jwt` module;
        # caching already seen tokens works best when a large volume of
        # messages have prompted a single push server to handle them, in which
        # case they would all share the same token for a limited time window.
        claim = id_token.verify_oauth2_token(
            token, requests.Request(), audience="example.com"
        )

        # IMPORTANT: you should validate claim details not covered by signature
        # and audience verification above, including:
        #   - Ensure that `claim["email"]` is equal to the expected service
        #     account set up in the push subscription settings.
        #   - Ensure that `claim["email_verified"]` is set to true.

        CLAIMS.append(claim)
    except Exception as e:
        return f"Invalid token: {e}\n", 400

    envelope = json.loads(request.data.decode("utf-8"))
    payload = base64.b64decode(envelope["message"]["data"])
    MESSAGES.append(payload)
    # Returning any 2xx status indicates successful receipt of the message.
    return "OK", 200
For information on the environment variable PUBSUB_VERIFICATION_TOKEN used in the code samples above, see Writing and responding to Pub/Sub messages.

Find additional examples of how to validate the bearer JWT in this Guide for Google Sign-in for Websites. A broader overview of OpenID tokens is available in the OpenID Connect Guide, including a list of client libraries that help validate JWTs.

Authentication from other Google Cloud services

Cloud Run and App Engine functions authenticate HTTP calls from Pub/Sub by verifying Pub/Sub-generated tokens. The only configuration that you require is to grant the necessary IAM roles to the caller account.

See the following guides and tutorials for different use cases with these services:

Cloud Run

Triggering from Pub/Sub push: Your push auth service account must have the roles/run.invoker role and be bound to a Cloud Run service to invoke a corresponding Cloud Run service
Using Pub/Sub with Cloud Run tutorial
App Engine

Writing and Responding to Pub/Sub Messages
Note: If your App Engine application is secured with Identity-Aware Proxy, you must grant your push auth service account the IAP-secured Web App User role on the App Engine application for Pub/Sub push requests to come through. See Add access for a detailed guide.
Cloud Run functions

HTTP Triggers: Your push auth service account must have the roles/cloudfunctions.invoker role to invoke a function if you intend to use Pub/Sub push requests as HTTP triggers to the function
Google Cloud Pub/Sub Triggers: IAM roles and permissions are auto-configured if you use Pub/Sub triggers to invoke a function
Note: In first-generation App Engine runtimes, push requests to endpoint URLs of the form /_ah/push-handlers/.* in the same project are authorized as admin users. This ACL mechanism is no longer supported in second generation App Engine runtimes. In second-generation App Engine runtimes, grant IAM roles for admin use to the push auth service account that your push subscription is configured with. For more details, visit Authenticating Users and Understanding Access Control.
