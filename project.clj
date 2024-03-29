(defproject b-social/event-processor "0.2.3-SNAPSHOT"

  :description "A library for processing events as a stuartsierra component and configured with configurati. Opinionated that the event processing will be wrapped in a jdbc transaction."

  :url "https://github.com/b-social/event-processor"

  :license {:name "The MIT License"
            :url  "https://opensource.org/licenses/MIT"}

  :dependencies [[org.clojure/clojure "1.10.0"]
                 [com.stuartsierra/component "1.0.0"]
                 [io.logicblocks/configurati "0.5.2"]
                 [cambium/cambium.core "0.9.3"]
                 [cambium/cambium.codec-cheshire "0.9.3"]
                 [org.clojure/java.jdbc "0.7.11"]
                 [org.clj-commons/claypoole "1.2.2"]
                 [com.datadoghq/dd-trace-ot "1.17.0"]]

  :plugins [[lein-cloverage "1.1.2"]
            [lein-shell "0.5.0"]
            [lein-ancient "0.6.15"]
            [lein-changelog "0.3.2"]
            [lein-eftest "0.5.9"]
            [lein-codox "0.10.7"]
            [lein-cljfmt "0.9.0"]
            [lein-kibit "0.1.8"]
            [lein-bikeshed "0.5.2"]]

  :profiles {:dev {:dependencies
                      [[eftest "0.5.9"]
                       [freeport "1.0.0"]
                       [com.opentable.components/otj-pg-embedded "1.0.1"]
                       [org.apache.curator/curator-test "5.0.0"]
                       [hikari-cp "2.12.0"]
                       [tortue/spy "2.0.0"]]
                    :eftest {:multithread? false}}
             :clj-kondo   {:plugins [[com.github.clj-kondo/lein-clj-kondo "0.2.1"]]}}

  :cloverage
  {:ns-exclude-regex [#"^user"]}

  :bikeshed {:max-line-length 100}

  :codox
  {:namespaces  [#"^event-processor\."]
   :metadata    {:doc/format :markdown}
   :output-path "docs"
   :doc-paths   ["docs"]
   :source-uri  "https://github.com/b-social/event-processor/blob/{version}/{filepath}#L{line}"}

  :repositories [["public-github" {:url "git://github.com"}]]

  :deploy-repositories [["github" {:url "https://maven.pkg.github.com/b-social/event-processor"
                                   :username :env/GITHUB_ACTOR
                                   :password :env/GITHUB_TOKEN}]]

  :cljfmt {:indents ^:replace {#".*" [[:inner 1]]}
           :sort-ns-references? true
           :remove-multiple-non-indenting-spaces? true
           :split-keypairs-over-multiple-lines? true}

  :aliases {"clj-kondo" ["with-profile" "+clj-kondo" "clj-kondo" "--lint" "src" "--lint" "test"]
            "pre-release" ["do" "cljfmt" "check," "clj-kondo," "check," "eftest"]}

  :release-tasks  [["pre-release"]
                   ["vcs" "assert-committed"]
                   ["change" "version" "leiningen.release/bump-version" "release"]
                   ["vcs" "commit"]
                   ["vcs" "tag"]
                   ["deploy" "github"]
                   ["change" "version" "leiningen.release/bump-version"]
                   ["vcs" "commit"]
                   ["vcs" "push"]])
