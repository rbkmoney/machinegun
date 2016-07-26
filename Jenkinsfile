#!groovy

// Args:
// GitHub repo name
// Jenkins agent label
// Tracing artifacts to be stored alongside build logs
pipeline("machinegun", 'docker-host', "_build/") {

  runStage('submodules') {
    sh 'make w_container_submodules'
  }

  runStage('compile') {
    sh 'make w_container_compile'
  }

  runStage('lint') {
    sh 'make w_container_lint'
  }

  runStage('xref') {
    sh 'make w_container_xref'
  }

  runStage('test') {
    sh "make w_compose_test"
  }

  runStage('dialyze') {
    sh 'make w_container_dialyze'
  }

  if (env.BRANCH_NAME == 'master') {
    runStage('push container') {
      sh 'make push'
    }
  }
}
