/*
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*     http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/
package org.apache.beam.example;

import org.apache.beam.runners.flink.FlinkPipelineOptions;
import org.apache.beam.runners.flink.FlinkRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringDelegateCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.*;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.transforms.join.CoGroupByKey;
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple;
import org.apache.beam.sdk.util.GcsUtil;
import org.apache.beam.sdk.util.gcsfs.GcsPath;
import org.apache.beam.sdk.values.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashSet;
import java.util.Set;

/**
 * An example that computes a basic Okapi-BM25 search table for a directory or GCS prefix.
 * <p>
 * <p> Concepts: joining data; side inputs; logging
 * <p>
 * <p> To execute this pipeline locally, specify general pipeline configuration:
 * <pre>{@code
 *   --project=YOUR_PROJECT_ID
 * }</pre>
 * and a local output file or output prefix on GCS:
 * <pre>{@code
 *   --output=[YOUR_LOCAL_FILE | gs://YOUR_OUTPUT_PREFIX]
 * }</pre>
 * <p>
 * <p> To execute this pipeline using the Dataflow service, specify pipeline configuration:
 * <pre>{@code
 *   --project=YOUR_PROJECT_ID
 *   --stagingLocation=gs://YOUR_STAGING_DIRECTORY
 *   --runner=BlockingDataflowRunner
 * and an output prefix on GCS:
 *   --output=gs://YOUR_OUTPUT_PREFIX
 * }</pre>
 * <p>
 * <p> The default input is {@code gs://dataflow-samples/shakespeare/} and can be overridden with
 * {@code --input}.
 */
public class BM25 {
  /**
   * Lists documents contained beneath the {@code options.input} prefix/directory.
   */
  public static Set<URI> listInputDocuments(BM25.Options options)
      throws URISyntaxException, IOException {
    URI baseUri = new URI(options.getInput());

    // List all documents in the directory or GCS prefix.
    URI absoluteUri;
    if (baseUri.getScheme() != null) {
      absoluteUri = baseUri;
    } else {
      absoluteUri = new URI(
          "file",
          baseUri.getAuthority(),
          baseUri.getPath(),
          baseUri.getQuery(),
          baseUri.getFragment());
    }

    Set<URI> uris = new HashSet<>();
    if (absoluteUri.getScheme().equals("file")) {
      File directory = new File(absoluteUri);
      for (String entry : directory.list()) {
        File path = new File(directory, entry);
        uris.add(path.toURI());
      }
    } else if (absoluteUri.getScheme().equals("gs")) {
      GcsUtil gcsUtil = options.as(GcsOptions.class).getGcsUtil();
      URI gcsUriGlob = new URI(
          absoluteUri.getScheme(),
          absoluteUri.getAuthority(),
          absoluteUri.getPath() + "*",
          absoluteUri.getQuery(),
          absoluteUri.getFragment());
      for (GcsPath entry : gcsUtil.expand(GcsPath.fromUri(gcsUriGlob))) {
        uris.add(entry.toUri());
      }
    }

    return uris;
  }

  /**
   * Options supported by {@link BM25}.
   * <p>
   * Inherits standard configuration options.
   */
  private interface Options extends PipelineOptions, FlinkPipelineOptions {
    @Description("Path to the directory or GCS prefix containing files to read from")
    @Default.String("gs://dataflow-samples/shakespeare/")
    String getInput();

    void setInput(String value);

    @Description("Prefix of output URI to write to")
    @Validation.Required
    @Default.String("/home/smarthi/bm25")
    String getOutput();

    void setOutput(String value);
  }

  /**
   * Reads the documents at the provided uris and returns all lines
   * from the documents tagged with which document they are from.
   */
  public static class ReadDocuments
      extends PTransform<PInput, PCollection<KV<URI, String>>> {
    private static final long serialVersionUID = 0;

    private Iterable<URI> uris;

    public ReadDocuments(Iterable<URI> uris) {
      this.uris = uris;
    }

    @Override
    public Coder<?> getDefaultOutputCoder() {
      return KvCoder.of(StringDelegateCoder.of(URI.class), StringUtf8Coder.of());
    }

    @Override
    public PCollection<KV<URI, String>> apply(PInput input) {
      Pipeline pipeline = input.getPipeline();

      // Create one TextIO.Read transform for each document
      // and add its output to a PCollectionList
      PCollectionList<KV<URI, String>> urisToLines =
          PCollectionList.empty(pipeline);

      // TextIO.Read supports:
      //  - file: URIs and paths locally
      //  - gs: URIs on the service
      for (final URI uri : uris) {
        String uriString;
        if (uri.getScheme().equals("file")) {
          uriString = new File(uri).getPath();
        } else {
          uriString = uri.toString();
        }

        PCollection<KV<URI, String>> oneUriToLines = pipeline
            .apply("TextIO.Read(" + uriString + ")", TextIO.Read.from(uriString))
            .apply("WithKeys(" + uriString + ")", WithKeys.<URI, String>of(uri));

        urisToLines = urisToLines.and(oneUriToLines);
      }

      return urisToLines.apply(Flatten.<KV<URI, String>>pCollections());
    }
  }

  public static class ComputeBM25 extends
      PTransform<PCollection<KV<URI, String>>, PCollection<KV<String, KV<URI, Double>>>> {
    private static final long serialVersionUID = 0L;
    // Instantiate the Logger
    private static final Logger LOG = LoggerFactory.getLogger(BM25.ComputeBM25.class);

    public ComputeBM25() {
    }

    @Override
    public PCollection<KV<String, KV<URI, Double>>> apply(
        PCollection<KV<URI, String>> uriToContent) {

      // Compute the total number of documents, and
      // prepare this singleton PCollectionView for
      // use as a side input.
      final PCollectionView<Long> totalDocuments =
          uriToContent
              .apply("GetURIs", Keys.<URI>create())
              .apply("RemoveDuplicateDocs", RemoveDuplicates.<URI>create())
              .apply(Count.<URI>globally())
              .apply(View.<Long>asSingleton());


      // Create a collection of pairs mapping a URI to each
      // of the words in the document associated with that that URI.
      PCollection<KV<URI, String>> uriToWords = uriToContent
          .apply("SplitWords", ParDo.of(new DoFn<KV<URI, String>, KV<URI, String>>() {
            private static final long serialVersionUID = 0;

            @Override
            public void processElement(ProcessContext c) {
              URI uri = c.element().getKey();
              String line = c.element().getValue();
              for (String word : line.split("\\W+")) {
                // Log INFO messages when the word “love” is found.
                if (word.toLowerCase().equals("love")) {
                  LOG.info("Found {}", word.toLowerCase());
                }

                if (!word.isEmpty()) {
                  c.output(KV.of(uri, word.toLowerCase()));
                }
              }
            }
          }));

      // Compute a mapping from each word to the total
      // number of documents in which it appears.
      PCollection<KV<String, Long>> wordToDocCount = uriToWords
          .apply("RemoveDuplicateWords", RemoveDuplicates.<KV<URI, String>>create())
          .apply(Values.<String>create())
          .apply("CountDocs", Count.<String>perElement());

      // Compute the total number of words in corpus,
      // and prepare this Singleton View as a side input
      // to calculate average document length across the corpus
      final PCollectionView<Long> totalWords =
          wordToDocCount
              .apply("GetWord", Keys.<String>create())
              .apply(Count.<String>globally())
              .apply(View.<Long>asSingleton());


      // Compute a mapping from each URI to the total
      // number of words in the document associated with that URI.
      final PCollection<KV<URI, Long>> uriToWordTotal = uriToWords
          .apply("GetURIs2", Keys.<URI>create())
          .apply("CountWords", Count.<URI>perElement());

      // Count, for each (URI, word) pair, the number of
      // occurrences of that word in the document associated
      // with the URI.
      PCollection<KV<KV<URI, String>, Long>> uriAndWordToCount = uriToWords
          .apply("CountWordDocPairs", Count.<KV<URI, String>>perElement());

      // Adjust the above collection to a mapping from
      // (URI, word) pairs to counts into an isomorphic mapping
      // from URI to (word, count) pairs, to prepare for a join
      // by the URI key.
      PCollection<KV<URI, KV<String, Long>>> uriToWordAndCount = uriAndWordToCount
          .apply("ShiftKeys", ParDo.of(
              new DoFn<KV<KV<URI, String>, Long>, KV<URI, KV<String, Long>>>() {
                private static final long serialVersionUID = 0;

                @Override
                public void processElement(ProcessContext c) {
                  URI uri = c.element().getKey().getKey();
                  String word = c.element().getKey().getValue();
                  Long occurrences = c.element().getValue();
                  c.output(KV.of(uri, KV.of(word, occurrences)));
                }
              }));

      // Prepare to join the mapping of URI to (word, count) pairs with
      // the mapping of URI to total word counts, by associating
      // each of the input PCollection<KV<URI, ...>> with
      // a tuple tag. Each input must have the same key type, URI
      // in this case. The type parameter of the tuple tag matches
      // the types of the values for each collection.
      // Perform a CoGroupByKey to yield a mapping from URI -> total
      // number of words in document and URI -> (word, count)
      final TupleTag<Long> wordTotalsTag = new TupleTag<>();
      final TupleTag<KV<String, Long>> wordCountsTag = new TupleTag<>();
      PCollection<KV<URI, CoGbkResult>> uriToWordAndCountAndTotal = KeyedPCollectionTuple
          .of(wordTotalsTag, uriToWordTotal)
          .and(wordCountsTag, uriToWordAndCount)
          .apply("CoGroupUri", CoGroupByKey.<URI>create());


      // Compute a mapping from each word to a (URI, (term frequency, docWordTotal))
      // pair for each URI. A word's term frequency for a document
      // is simply the number of times that word occurs in the document
      // divided by the total number of words in the document.
      PCollection<KV<String, KV<URI, KV<Double, Long>>>> wordToUriAndTf =
          uriToWordAndCountAndTotal
              .apply("ComputeTermFrequencies", ParDo.of(
                  new DoFn<KV<URI, CoGbkResult>, KV<String, KV<URI, KV<Double, Long>>>>() {

                    @Override
                    public void processElement(ProcessContext c) throws Exception {
                      URI uri = c.element().getKey();
                      Long docWordTotal = c.element().getValue().getOnly(wordTotalsTag);

                      for (KV<String, Long> wordAndCount :
                          c.element().getValue().getAll(wordCountsTag)) {
                        String word = wordAndCount.getKey();
                        Long wordCount = wordAndCount.getValue();
                        Double termFrequency = wordCount.doubleValue() / docWordTotal;
                        c.output(KV.of(word, KV.of(uri, KV.of(termFrequency, docWordTotal))));
                      }
                    }
                  }));

      // Compute a mapping from each word to its document frequency.
      // A word's document frequency in a corpus is the number of
      // documents in which the word appears divided by the total
      // number of documents in the corpus.
      PCollection<KV<String, Double>> wordToDf = wordToDocCount
          .apply("ComputeDocFrequencies", ParDo
              .withSideInputs(totalDocuments)
              .of(new DoFn<KV<String, Long>, KV<String, Double>>() {
                private static final long serialVersionUID = 0;

                @Override
                public void processElement(ProcessContext c) {
                  String word = c.element().getKey();
                  Long documentCount = c.element().getValue();
                  Long documentTotal = c.sideInput(totalDocuments);
                  Double documentFrequency = documentCount.doubleValue()
                      / documentTotal.doubleValue();

                  c.output(KV.of(word, documentFrequency));
                }
              }));

      // Join the term frequency and document frequency
      // collections, each keyed on the word.
      final TupleTag<KV<URI, KV<Double, Long>>> tfAndWordTotalTag = new TupleTag<>();
      final TupleTag<Double> dfTag = new TupleTag<>();
      PCollection<KV<String, CoGbkResult>> wordToUriAndBM25 = KeyedPCollectionTuple
          .of(tfAndWordTotalTag, wordToUriAndTf)
          .and(dfTag, wordToDf)
          .apply(CoGroupByKey.<String>create());

      // Compute a mapping from each word to a (URI, BM25) score
      // for each URI.
      // See {@link https://en.wikipedia.org/wiki/Okapi_BM25}
      return wordToUriAndBM25
          .apply("ComputeBM25", ParDo.of(
              new DoFn<KV<String, CoGbkResult>, KV<String, KV<URI, Double>>>() {
                private static final long serialVersionUID = 0;

                @Override
                public void processElement(ProcessContext c) {
                  String word = c.element().getKey();
                  Double df = c.element().getValue().getOnly(dfTag);
                  Long wordTotal = c.sideInput(totalWords);
                  Long docTotal = c.sideInput(totalDocuments);
                  Double avgDocLength = wordTotal.doubleValue() / docTotal;
                  Double k1 = 1.2;
                  Double b = 0.75;

                  for (KV<URI, KV<Double, Long>> uriToTfAndDocWordTotal :
                      c.element().getValue().getAll(tfAndWordTotalTag)) {
                    URI uri = uriToTfAndDocWordTotal.getKey();
                    Double tf = uriToTfAndDocWordTotal.getValue().getKey();
                    Long docLength = uriToTfAndDocWordTotal.getValue().getValue();

                    Double bm25 = Math.log(1/df) *
                        (tf * (k1 + 1)) / (tf + k1 * (1 - b + b * Math.abs(docLength) / avgDocLength));

                    c.output(KV.of(word, KV.of(uri, bm25)));
                  }
                }
              }));

    }
  }

  /**
   * A {@link PTransform} to write, in CSV format, a mapping from term and URI
   * to score.
   */
  public static class WriteBM25
      extends PTransform<PCollection<KV<String, KV<URI, Double>>>, PDone> {
    private static final long serialVersionUID = 0;

    private String output;

    public WriteBM25(String output) {
      this.output = output;
    }

    @Override
    public PDone apply(PCollection<KV<String, KV<URI, Double>>> wordToUriAndBM25) {
      return wordToUriAndBM25
          .apply("Format", ParDo.of(new DoFn<KV<String, KV<URI, Double>>, String>() {
            private static final long serialVersionUID = 0;

            @Override
            public void processElement(ProcessContext c) {
              c.output(String.format("%s,\t%s,\t%f",
                  c.element().getKey(),
                  c.element().getValue().getKey(),
                  c.element().getValue().getValue()));
            }
          }))
          .apply(TextIO.Write
              .to(output)
              .withSuffix(".csv"));
    }
  }

  public static void main(String[] args) throws Exception {
    BM25.Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(BM25.Options.class);

    options.setRunner(FlinkRunner.class);

    Pipeline pipeline = Pipeline.create(options);
    pipeline.getCoderRegistry().registerCoder(URI.class, StringDelegateCoder.of(URI.class));

    pipeline.apply(new BM25.ReadDocuments(listInputDocuments(options)))
        .apply(new BM25.ComputeBM25())
        .apply(new BM25.WriteBM25(options.getOutput()));

    pipeline.run();
  }

}
