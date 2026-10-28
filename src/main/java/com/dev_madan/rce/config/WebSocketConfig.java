package com.dev_madan.rce.config;
import java.util.List;

import org.springframework.context.annotation.Configuration;
import org.springframework.messaging.converter.JacksonJsonMessageConverter ;
import org.springframework.messaging.converter.MessageConverter;
import org.springframework.messaging.simp.config.MessageBrokerRegistry;
import org.springframework.web.socket.config.annotation.EnableWebSocketMessageBroker;
import org.springframework.web.socket.config.annotation.StompEndpointRegistry;
import org.springframework.web.socket.config.annotation.WebSocketMessageBrokerConfigurer;

@Configuration
@EnableWebSocketMessageBroker // turn on STOMP support
public class WebSocketConfig implements WebSocketMessageBrokerConfigurer {

    @Override
    public void registerStompEndpoints(StompEndpointRegistry registry){
        // This method does the same — it attaches a WebSocket endpoint to the HTTP server Spring is already running.
        // /ws is the URL path. Your React client will connect to ws://localhost:5000/ws

        // setAllowedOriginPatterns("*") = app.use(cors()). Same thing.

        registry.addEndpoint("/ws")
                .setAllowedOriginPatterns("*")
                .withSockJS();
    }

    @Override
    public void configureMessageBroker(MessageBrokerRegistry registry){
        // This sets up an in-memory message broker for the server→client direction. In Node we did:
        // ws.send(JSON.stringify({ type: 'stdout', data: chunk }))
        // We held a reference to ws and pushed directly. In Spring with STOMP, you instead publish to a topic and any client subscribed to it gets the message:
        registry.enableSimpleBroker("/topic");

        // when calling a route from frontend, The /topic prefix would tell Spring's broker: this is a subscription destination, not a controller route.

        // This says: when the client sends a message to a destination starting with /app, route it to a @MessageMapping method in a controller.
        // in react - client.publish({ destination: '/app/run', body: JSON.stringify({ lang, code }) })
        // Spring sees /app/run, strips the /app prefix, and looks for @MessageMapping("/run") — exactly like Express routing:

        registry.setApplicationDestinationPrefixes("/app");
    }

    @Override
    public boolean configureMessageConverters(List<MessageConverter> messageConverters) {
        messageConverters.add(new JacksonJsonMessageConverter());
        return false;
    }
}

//    Node.js                              Spring Boot
//    ──────────────────────────────────────────────────────────
//            new WebSocket.Server({ server })  →  @EnableWebSocketMessageBroker
//                                         + registerStompEndpoints("/ws")
//
//    ws.on('message', msg => {         →  @MessageMapping("/run")
//        if (type === 'run') { ... }        @MessageMapping("/stdin")
//        if (type === 'stdin') { ... }
//    })
//
//            ws.send(JSON.stringify({...}))    →  messagingTemplate
//            .convertAndSend("/topic/output/"+sessionId, payload)
//
//    // client side:
//    new WebSocket('ws://localhost:5000') → new SockJS('http://localhost:5000/ws')
//    ws.onmessage = (e) => {...}          client.subscribe('/topic/output/'+sessionId, ...)
