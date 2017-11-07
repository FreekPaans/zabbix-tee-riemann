(ns zabbix-tee-riemann.core
  (:require
    [clojure.java.io :as io]
    [clojure.data.json :as json]
    [clojure.pprint :refer [pprint]])
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
    [io.netty.handler.codec.bytes ByteArrayEncoder])
  (:gen-class))

(defn ->json-bytes [msg]
  (let [json-byte-stream (ByteArrayOutputStream.)]
    (with-open [w (io/writer json-byte-stream)]
      (do
        (json/write msg w)
        (.flush w)
        (.toByteArray json-byte-stream)))))

(defn long->zabbix-bytes [val]
  (let [byte-buf (ByteBuffer/allocate 8)]
    (.order byte-buf (ByteOrder/LITTLE_ENDIAN))
    (.putLong byte-buf (long val))
    (.array byte-buf)))

(defn zabbix-bytes->long [bytes]
  (let [byte-buf (ByteBuffer/allocate 8)]
    (.order byte-buf (ByteOrder/LITTLE_ENDIAN))
    (.put byte-buf (byte-array bytes))
    (.getLong byte-buf 0)))

(defn map->zabbix-msg-bytes [msg]
  (let [json-bytes (->json-bytes msg)
        result-bytes (ByteArrayOutputStream.)]
    (doto result-bytes
      (.write (byte-array (map int (seq "ZBXD"))))
      (.write (int 1))
      (.write (long->zabbix-bytes (count json-bytes)))
      (.write json-bytes)
      (.flush))
    (.toByteArray result-bytes)))

(defn zabbix-msg-decoder-netty-inbound-handler []
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
                 (zabbix-bytes->long)))

          (read-msg->byte-array! [msg content-length]
            (.skipBytes msg 13)
            (let [content-buf (byte-array content-length)]
              (.readBytes msg content-buf)
              content-buf))]

    (proxy [ByteToMessageDecoder] []
      (decode [ctx msg out]
        (let [readable-bytes (.readableBytes msg)]
          (when (>= readable-bytes 13)
            (let [header-bytes (byte-array readable-bytes)]
              (do
                (.getBytes msg 0 header-bytes)
                (let [content-length (verify-header-and-get-length
                                       (take 13 (vec header-bytes)))]
                  (when (>= readable-bytes (+ 13 content-length))
                    (.add out
                          (read-msg->byte-array! msg content-length))))))))))))

(defn logging-handler-netty-duplex-handler [name]
  (LoggingHandler. name LogLevel/INFO))

(defn netty-channel-future-listener [callback]
  (reify ChannelFutureListener
    (operationComplete [this fut]
      (callback fut))))

(defn map-netty-outbound-handler [f]
  (proxy [ChannelOutboundHandlerAdapter] []
    (write [ctx msg promise]
      (.writeAndFlush ctx (f msg) promise))))

(defn forward-netty-inbound-handler [dest-channel]
  (proxy [ChannelInboundHandlerAdapter] []
    (channelInactive [ctx]
      (.close dest-channel))
    (channelRead [ctx msg]
      (.writeAndFlush dest-channel msg))))

(defn bytes->json-netty-inbound-handler []
  (proxy [ChannelInboundHandlerAdapter] []
    (channelRead [ctx msg]
      (with-open [rdr (io/reader msg)]
        (let [json (json/read rdr)]
          (.fireChannelRead ctx json))))))

(defn print-netty-inbound-handler []
  (proxy [ChannelInboundHandlerAdapter] []
    (channelRead [ctx msg]
      (pprint msg)
      (.fireChannelRead ctx msg))))

(defn zabbix-client-netty-bootstrap
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

(defn forward-to-zabbix-server-netty-inbound-handler
  [client-handlers]
  (let [outgoing-channel (atom nil)]
    (proxy [ChannelInboundHandlerAdapter] []
      (channelActive [ctx]
        (let [incoming-channel (.. ctx channel )
              client (zabbix-client-netty-bootstrap
                       (.eventLoop incoming-channel)
                       (.getClass incoming-channel)
                       (partial client-handlers incoming-channel))
              connect-result (.. client (connect "ubuntu-xenial" 10051))]
            (.addListener connect-result
                          (netty-channel-future-listener
                            (fn [_]
                              (reset! outgoing-channel  (.channel connect-result))
                              (.read incoming-channel))))))
        (channelRead [ctx msg]
          (.writeAndFlush @outgoing-channel msg)))))

(defn server-bootstrap [group handlers-factory]
  (let [bootstrap (ServerBootstrap.)]
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
        (childOption ChannelOption/AUTO_READ false))
    bootstrap))

(defn client-handlers [forward-to-channel]
  [
   ;(logging-handler "client")
   (forward-netty-inbound-handler forward-to-channel)
   (ByteArrayEncoder.)
   (map-netty-outbound-handler map->zabbix-msg-bytes)])

(defn make-server-handlers []
  [
   ;(logging-handler "before-decode")
   (zabbix-msg-decoder-netty-inbound-handler)
   (bytes->json-netty-inbound-handler)
   (print-netty-inbound-handler)
   (forward-to-zabbix-server-netty-inbound-handler client-handlers)
   ;(logging-handler "after-decode")
   ])

(defn start-server [port]
  (let [event-loop-group (NioEventLoopGroup.)
        bootstrap (server-bootstrap event-loop-group (var make-server-handlers))]
    (let [channel (..
                    (.bind bootstrap port)
                    (sync)
                    (channel))]
      (.. channel
          closeFuture
          (addListener
            (netty-channel-future-listener
              (fn [fut]
                (.shutdownGracefully event-loop-group)))))
      channel)))

(defn -main [& args]
  (start-server 9002))

(comment
  (def server (start-server 9002))
  (.close server))
