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
    [io.netty.buffer Unpooled]
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
    (.order byte-buf ByteOrder/LITTLE_ENDIAN)
    (.putLong byte-buf (long val))
    (.array byte-buf)))

(defn zabbix-bytes->long [bytes]
  (let [byte-buf (ByteBuffer/allocate 8)]
    (.order byte-buf ByteOrder/LITTLE_ENDIAN)
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

(defn logging-netty-duplex-handler [name]
  (LoggingHandler. name LogLevel/INFO))

(defn netty-channel-future-listener [callback]
  (reify ChannelFutureListener
    (operationComplete [this fut]
      (callback fut))))

(defn map-netty-outbound-handler [f]
  (proxy [ChannelOutboundHandlerAdapter] []
    (write [ctx msg promise]
      (.writeAndFlush ctx (f msg) promise))))

(defn flush-and-close! [channel]
  (-> channel
      (.writeAndFlush Unpooled/EMPTY_BUFFER)
      (.addListener ChannelFutureListener/CLOSE)))

(defn on-complete [netty-future complete-fn]
  (.addListener netty-future
    (netty-channel-future-listener complete-fn)))

(defn forward-netty-inbound-handler [zbx-agent-channel]
  (proxy [ChannelInboundHandlerAdapter] []
    (channelActive [ctx]
      (.. ctx read))
    (channelInactive [ctx]
      (flush-and-close! zbx-agent-channel))
    (channelRead [ctx msg]
      (let [zbx-server-channel (.channel ctx)]
        (-> zbx-agent-channel
            (.writeAndFlush msg)
            (on-complete
              (fn [result]
                (if (.isSuccess result)
                  (.read zbx-server-channel)
                  (do
                    (println "failed writing to agent")
                    (.printStackTrace (.cause result))
                    (flush-and-close! zbx-server-channel))))))))))

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

(defn read-channel-netty-inbound-handler [on-read]
  (proxy [ChannelInboundHandlerAdapter] []
    (channelRead [ctx msg]
      (on-read ctx msg))))

(defn channel-active-netty-inbound-handler [on-active]
  (proxy [ChannelInboundHandlerAdapter] []
    (channelActive [ctx]
      (on-active ctx ))))

(defn close-on-exception-netty-inbound-handler []
  (proxy [ChannelInboundHandlerAdapter] []
    (exceptionCaught [ctx cause]
      (.printStackTrace cause) ;TODO add proper logging
      (.. ctx channel close))))

(defn proxy-client-netty-bootstrap
  [event-loop-group channel-class handlers-factory]
  (let [bootstrap (Bootstrap.)]
    (doto bootstrap
      (.group event-loop-group)
      (.channel channel-class)
      (.option ChannelOption/SO_KEEPALIVE true)
      (.option ChannelOption/AUTO_READ false)
      (.option ChannelOption/AUTO_CLOSE false)
      (.handler
        (proxy [ChannelInitializer] []
          (initChannel [channel]
            (.. channel
                pipeline
                (addLast (into-array ChannelHandler (handlers-factory))))
            ))))
    bootstrap))

(defn proxy-netty-inbound-handler
  [client-handlers-factory]
  (let [zbx-server-channel (atom nil)]
    (proxy [ChannelInboundHandlerAdapter] []
      (channelActive [ctx]
        (let [incoming-channel (.. ctx channel)
              client (proxy-client-netty-bootstrap
                       (.eventLoop incoming-channel)
                       (.getClass incoming-channel)
                       (partial client-handlers-factory incoming-channel))]
          (-> client
              (.connect "ubuntu-xenial" 10051)
              (on-complete
                (fn [connect-result]
                  (if (.isSuccess connect-result)
                    (do
                      (reset! zbx-server-channel  (.channel connect-result))
                      (.read incoming-channel))
                    (do
                      (println "Failed connecting to zabbix server") ; TODO logging
                      (.printStackTrace (.cause connect-result))
                      (.close incoming-channel))))))))
      (channelRead [ctx msg]
        (let [zbx-agent-channel (.channel ctx)]
          (-> @zbx-server-channel
              (.writeAndFlush msg)
              (on-complete
                (fn [result]
                  (if (.isSuccess result)
                    (.read zbx-agent-channel)
                    (do
                      (println "failed writing to server")
                      (.printStackTrace (.cause result))
                      (flush-and-close! zbx-agent-channel))))))))
      (channelInactive [ctx]
        (flush-and-close! @zbx-server-channel)))))

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
        (childOption ChannelOption/AUTO_READ false)
        (childOption ChannelOption/AUTO_CLOSE false)
        )
    bootstrap))

(defn start-server [port handlers-factory]
  (let [event-loop-group (NioEventLoopGroup.)
        bootstrap (server-bootstrap event-loop-group handlers-factory)]
    (let [channel (.. bootstrap
                    (bind port)
                    (sync)
                    (channel))]
      (-> channel
          .closeFuture
          (on-complete
            (fn [fut]
              (.shutdownGracefully event-loop-group))))
      channel)))

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
   (proxy-netty-inbound-handler client-handlers)
   ;(logging-netty-duplex-handler  "after-decode")
   ])

(defn start-zabbix-proxy-server [port]
  (start-server 9002 (var make-server-handlers)))

(defn -main [& args]
  (start-zabbix-proxy-server 9002))

(comment
  (def server (start-zabbix-proxy-server 9002))
  (.close server)
  )
