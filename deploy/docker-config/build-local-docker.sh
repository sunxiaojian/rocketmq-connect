#!/bin/bash

# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

latestVersion=0.0.1-SNAPSHOT
app=rocketmq-connect
full_app=$app-$latestVersion
repository=********/$app

base_dir=$(dirname $0)
echo $base_dir
dir_path=$(cd `dirname $0`; pwd)
echo $dir_path

## mvn build project
function package() {
  cd $base_dir && cd ../../ && mvn -Prelease-connect -DskipTests clean install -U && pwd
  target="distribution/target/$full_app/$full_app"
  while [ ! -e $target ]; do
      echo "$target not exist!!"
      sleep 5
  done
  mv -v distribution/target/$full_app/$full_app deploy/docker-config/
}

## docker push
function docker_push() {
  cd $dir_path

  ## build docker image
  echo docker build -f Dockerfile -t $repository:$latestVersion
  docker build -f Dockerfile -t $repository:$latestVersion .

  ## push docker image
  echo docker push $repository:$latestVersion
  docker push $repository:$latestVersion

  if [ -e $full_app ]; then
    echo "Remove file $full_app"
    rm -rf $full_app
  fi
}

## maven package
package
## docker push
docker_push