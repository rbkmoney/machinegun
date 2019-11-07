#!groovy

//
// Copyright 2017 RBKmoney
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//

def finalHook = {
  runStage('store CT logs') {
    archive '_build/test/logs/'
  }
}

build('machinegun', 'docker-host', finalHook) {
  checkoutRepo()
  loadBuildUtils()

  def pipeDefault
  def withWsCache
  runStage('load pipeline') {
    env.JENKINS_LIB = "build_utils/jenkins_lib"
    pipeDefault = load("${env.JENKINS_LIB}/pipeDefault.groovy")
    withWsCache = load("${env.JENKINS_LIB}/withWsCache.groovy")
  }

  pipeDefault() {
    runStage('compile') {
      withGithubPrivkey{
          sh 'make wc_compile'
      }
    }
    runStage('lint') {
      sh 'make wc_lint'
    }
    runStage('xref') {
      sh 'make wc_xref'
    }
    runStage('dialyze') {
      withWsCache("_build/default/rebar3_21.3.8.7_plt") {
        sh 'make wc_dialyze'
      }
    }
    runStage('test') {
      sh "make wc_test_configurator"
      sh "make wdeps_test"
    }
    runStage('make release') {
      withGithubPrivkey{
        sh "make wc_release"
      }
    }
    runStage('build image') {
      sh "make build_image"
    }

    try {
      if (masterlikeBranch()) {
        runStage('push image') {
          sh "make push_image"
        }
      }
    } finally {
      runStage('rm local image') {
        sh 'make rm_local_image'
      }
    }
  }
}
