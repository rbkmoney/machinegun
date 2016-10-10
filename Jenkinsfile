#!groovy

def finalHook = {
  runStage('store CT logs') {
    archive '_build/test/logs/'
  }
}

build('machinegun', 'docker-host', finalHook) {
  checkoutRepo()
  loadBuildUtils()

  def pipeDefault
  runStage('load pipeline') {
    env.JENKINS_LIB = "build_utils/jenkins_lib"
    pipeDefault = load("${env.JENKINS_LIB}/pipeDefault.groovy")
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
      sh 'make wc_dialyze'
    }
    runStage('test') {
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
      if (env.BRANCH_NAME == 'master') {
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
