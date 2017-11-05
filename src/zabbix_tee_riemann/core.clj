(ns zabbix-tee-riemann.core
  (:require
    [clojure.java.io :as io]
    [clojure.data.json :as json])
  (:import
    [io.netty.bootstrap ServerBootstrap Bootstrap]
    [io.netty.channel.socket.nio NioServerSocketChannel]
    [io.netty.channel ChannelHandlerContext ChannelInboundHandlerAdapter
     ChannelInitializer ChannelOption ChannelHandler ChannelFutureListener
     ChannelOutboundHandlerAdapter]
    [io.netty.channel.nio NioEventLoopGroup]
    [io.netty.handler.codec ByteToMessageDecoder]
    [io.netty.handler.logging LoggingHandler LogLevel]
    [java.nio ByteBuffer ByteOrder]
    [java.io ByteArrayOutputStream]
    [io.netty.handler.codec.bytes ByteArrayEncoder]
    ))

(defn foo
  "I don't do a whole lot."
  [x]
  (println x "Hello, World!"))

(defn make-server-bootstrap [group handlers-factory]
  (let [bootstrap (ServerBootstrap.)]
    (do
      (.. bootstrap
          (group group)
          (channel NioServerSocketChannel)
          (childHandler
            (proxy [ChannelInitializer] []
              (initChannel [channel]
                (.. channel
                    (pipeline)
                    (addLast (into-array ChannelHandler (handlers-factory)))))))
          (option ChannelOption/SO_BACKLOG (int 128))
          (childOption ChannelOption/SO_KEEPALIVE true)
          (childOption ChannelOption/AUTO_READ false)))
    bootstrap))


(defn long->zabbix-bytes [val]
  (let [byte-buf (ByteBuffer/allocate 8)]
    (.order byte-buf (ByteOrder/LITTLE_ENDIAN))
    (.putLong byte-buf (long val))
    (.array byte-buf)))

(defn zabbix-bytes->long [bytes]
  (let [byte-buf (ByteBuffer/allocate 8)]
    (.order byte-buf (ByteOrder/LITTLE_ENDIAN))
    (.put byte-buf (byte-array bytes))
    (.getLong  byte-buf 0)))

(defn zabbix-msg-extractor []
  (letfn [(assert [pred msg buffer]
            (when-not pred
              (throw (ex-info msg {:buffer buffer}))))
          (verify-header-and-get-length  [buffer]
            (assert (= (count buffer) 13) "header should be 13 characters" buffer)
            (assert (= (take 4 buffer) (map byte (seq "ZBXD"))) "Expected ZBXD as first characters" buffer)
            (assert (= (nth buffer 4) (byte 1)) "5th byte should be 1" buffer)
            (->> buffer
                 (drop 5)
                 (take 8)
                 (zabbix-bytes->long)))]
    (proxy [ByteToMessageDecoder] []
      (decode [ctx msg out]
        (let [readable-bytes (.readableBytes msg)]
          (when (>= readable-bytes 13)
            (let [header-bytes (byte-array readable-bytes)]
              (do
                (.getBytes msg 0 header-bytes)
                (let [content-length (verify-header-and-get-length (take 13 (vec header-bytes)))]
                  (when (>= readable-bytes (+ 13 content-length))
                    (do
                      (.skipBytes msg 13)
                      (let [content-buf (byte-array content-length)]
                        (do
                          (.readBytes msg content-buf)
                          (.add out content-buf))))))))))))))

(defn logging-handler [name]
  (LoggingHandler. name LogLevel/INFO))

(defn channel-future-listener [callback]
  (reify ChannelFutureListener
    (operationComplete [this fut]
      (callback  fut))))

(defn make-zabbix-client
  [event-loop-group channel-class handlers-factory]
  (let [bootstrap (Bootstrap.)]
    (doto bootstrap
      (.group event-loop-group)
      (.channel channel-class)
      (.option ChannelOption/SO_KEEPALIVE true)
      (.handler
        (proxy [ChannelInitializer] []
          (initChannel [channel]
            (.. channel
                pipeline
                (addLast (into-array ChannelHandler (handlers-factory))))))))
    bootstrap))

(defn ->json-bytes [msg]
  (let [json-byte-stream (ByteArrayOutputStream.)]
    (with-open [w (io/writer json-byte-stream)]
      (do
        (json/write msg w)
        (.flush w)
        (.toByteArray json-byte-stream)))))

(defn encode-zabbix-msg [msg]
  (let [json-bytes (->json-bytes msg)
        result-bytes (ByteArrayOutputStream.)]
    (doto result-bytes
      (.write (byte-array (map int (seq "ZBXD"))))
      (.write (int 1))
      (.write (long->zabbix-bytes (count json-bytes)))
      (.write json-bytes)
      (.flush))
    (.toByteArray result-bytes)))

(defn zabbix-encoder []
  (proxy [ChannelOutboundHandlerAdapter] []
    (write [ctx msg promise]
      (.writeAndFlush ctx (encode-zabbix-msg msg) promise))))

(defn forwarder [dest-channel]
  (proxy [ChannelInboundHandlerAdapter] []
    (channelInactive [ctx]
      (.close dest-channel))
    (channelRead [ctx msg]
      (.writeAndFlush dest-channel msg))))


(defn client-handlers [forward-to-channel]
  [
   ;(logging-handler "client")
   (forwarder forward-to-channel)
   (ByteArrayEncoder.)
   (zabbix-encoder)])

(defn zabbix-forwarder []
  (let [client-channel (atom nil)]
    (proxy [ChannelInboundHandlerAdapter] []
      (channelActive [ctx]
        (let [client (make-zabbix-client (.. ctx channel eventLoop)
                                         (.. ctx channel getClass)
                                         (partial client-handlers (.. ctx channel)))
              connect-result (.. client (connect "ubuntu-xenial" 10051))]
            (.addListener connect-result
                          (channel-future-listener
                            (fn [_]
                              (reset! client-channel  (.channel connect-result))
                              (.. ctx channel read))))))

        (channelRead [ctx msg]
          (.writeAndFlush @client-channel msg)))))

(defn bytes->json-decoder []
  (proxy [ChannelInboundHandlerAdapter] []
    (channelRead [ctx msg]
      (with-open [rdr (io/reader msg)]
        (let [json (json/read rdr)]
          (.fireChannelRead ctx json))))))


(defn echo-handler []
  (proxy [ChannelInboundHandlerAdapter] []
    (channelRead [ctx msg]
      (println msg)
      (.fireChannelRead ctx msg))))

(defn make-server-handlers []
  [
   ;(logging-handler "before-decode")
   (zabbix-msg-extractor)
   (bytes->json-decoder)
   (echo-handler)
   (zabbix-forwarder)
   ;(logging-handler "after-decode")
   ])

(defn start-server [port]
  (let [event-loop-group (NioEventLoopGroup.)
        bootstrap (make-server-bootstrap event-loop-group (var make-server-handlers))]
    (let [channel (..
                    (.bind bootstrap port)
                    (sync)
                    (channel))]
      (.. channel
          closeFuture
          (addListener
            (channel-future-listener
              (fn [fut]
                (.shutdownGracefully event-loop-group)))))
      channel)))

(comment
  (def server (start-server 9002))
  (.close server))
