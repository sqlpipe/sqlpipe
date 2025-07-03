FROM debian:12

# Install required tools for HTTPS repositories and GPG
RUN apt-get update && apt-get install -y \
  ca-certificates \
  curl \
  gnupg

RUN curl -s https://packages.stripe.dev/api/security/keypair/stripe-cli-gpg/public | gpg --dearmor | tee /usr/share/keyrings/stripe.gpg
RUN echo "deb [signed-by=/usr/share/keyrings/stripe.gpg] https://packages.stripe.dev/stripe-cli-debian-local stable main" | tee -a /etc/apt/sources.list.d/stripe.list

RUN apt-get update
RUN apt-get install stripe -y

RUN rm -rf /var/lib/apt/lists/*

COPY /bin/streaming /bin/streaming
COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

EXPOSE 4000

ENTRYPOINT ["/entrypoint.sh"]
