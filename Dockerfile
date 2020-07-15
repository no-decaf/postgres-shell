FROM postgres:10
RUN apt-get update
RUN apt-get install -y less vim
COPY .psqlrc /root/.psqlrc
ENV EDITOR=vim
ENV PGPASSWORD=Get@round123!
CMD psql \
    -h postgres.data.getaround.com \
    -p 5432 \
    -U aaron_drenberg \
    -d getaround
