/**
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

package org.apache.hadoop.yarn.server.resourcemanager.applicationsmanager;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.event.Dispatcher;
import org.apache.hadoop.yarn.event.Event;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.service.AbstractService;


public class SyncDispatcher extends AbstractService implements Dispatcher {
  private Configuration conf;
  private static final Log LOG = LogFactory.getLog(SyncDispatcher.class);
  @SuppressWarnings("rawtypes")
  private Map<Class<? extends Enum>, EventHandler> eventDispatchers = 
    new HashMap<Class<? extends Enum>, EventHandler>();

  
  public SyncDispatcher() {
    super("SyncDispatcher");
  }
  
  @Override
  public void init(Configuration conf) {
    this.conf = conf;
  }

  @SuppressWarnings("rawtypes")
  @Override
  public void register(Class<? extends Enum> eventType,
      EventHandler handler) {
    /* check to see if we have a listener registered */
      @SuppressWarnings("unchecked")
      EventHandler<Event> registeredHandler = (EventHandler<Event>)
      eventDispatchers.get(eventType);
      LOG.info("Registering " + eventType + " for " + handler.getClass());
      if (registeredHandler == null) {
        eventDispatchers.put(eventType, handler);
      } else if (!(registeredHandler instanceof MultiListenerHandler)){
        /* for multiple listeners of an event add the multiple listener handler */
        MultiListenerHandler multiHandler = new MultiListenerHandler();
        multiHandler.addHandler(registeredHandler);
        multiHandler.addHandler(handler);
        eventDispatchers.put(eventType, multiHandler);
      } else {
        /* already a multilistener, just add to it */
        MultiListenerHandler multiHandler
        = (MultiListenerHandler) registeredHandler;
        multiHandler.addHandler(handler);
      }
  }
  
  @SuppressWarnings({ "rawtypes", "unchecked" })
  @Override
  public EventHandler<Event> getEventHandler() {
    return new GenericEventHandler();
  }
  
  class GenericEventHandler implements EventHandler {

    @SuppressWarnings("unchecked")
    @Override
    public void handle(Event event) {
      if (!LOG.isDebugEnabled()) {
        LOG.info("Dispatching event " + event);
      }
      Class<? extends Enum> type = event.getType().getDeclaringClass();
      
      try{
        EventHandler<Event> handler = 
          eventDispatchers.get(type);
        if (handler == null)  
            throw new IllegalArgumentException("Invalid event " + event
                + " no registered handler.");
        handler.handle(event);
      }
      catch (Throwable t) {
        LOG.fatal("Error in dispatcher", t);
        //TODO fix the shutdown
      }
    }
  }

  /**
   * Multiplexing an event. Sending it to different handlers that
   * are interested in the event.
   * @param <T> the type of event these multiple handlers are interested in.
   */
  @SuppressWarnings("rawtypes")
  static class MultiListenerHandler implements EventHandler<Event> {
    List<EventHandler<Event>> listofHandlers;
    
    public MultiListenerHandler() {
      listofHandlers = new ArrayList<EventHandler<Event>>();
    }
    
    @Override
    public void handle(Event event) {
      for (EventHandler<Event> handler: listofHandlers) {
        handler.handle(event);
      }
    }
    
    void addHandler(EventHandler<Event> handler) {
      listofHandlers.add(handler);
    }
  }
}