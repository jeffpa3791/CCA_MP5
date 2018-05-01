import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;

import java.io.IOException;

/**
 * Implementation of the connected component algorithm that identifies
 * connected components and assigns each vertex its "component
 * identifier" (the smallest vertex id in the component).
 */
public class ConnectedComponentsComputation extends
    BasicComputation<IntWritable, IntWritable, NullWritable, IntWritable> {
  /**
   * Propagates the smallest vertex id to all neighbors. Will always choose to
   * halt and only reactivate if a smaller id has been sent to it.
   *
   * @param vertex Vertex
   * @param messages Iterator of messages from the previous superstep.
   * @throws IOException
   */
  @Override
  public void compute(
      Vertex<IntWritable, IntWritable, NullWritable> vertex,
      Iterable<IntWritable> messages) throws IOException {
      //TODO
      
      // get the value of the vertex
      int currentValue = vertex.getValue().get();
      
      // first superstep,
      if (getSuperStep() == 0) {
        for (IntWritable neighbor : vertex.getNeighbors()) {
          if (neighbor.get() < currentValue) {
            currentValue = neighbor.get();
          }
        }
        
        // currentValue is now either what we started with or the lowest neighbor
        // if we need to change, send a message and that will start the process rolling
        if (currentValue != vertex.getValue().get()) {
          vertex.setValue(new IntWritable(currentValue));
          // send message to all the neighbors
          for (IntWritable neighbor : vertex.getNeighbors()) {
            if (neighbor.get() > currentValue) {
              SendMessage(new IntWritable(neighbor.get()), vertex.getValue() )            }  
          }
        }
        
        // this vertex is inactive unless it receives a message
        vertex.voteToHalt();
        return;
        
      } // end of code for first superstep
     
      // code when it is a later superstep
      // the vertex has received one/more messages to process
      
      boolean changed = false;    
      for (IntWritable message : messages) {
        int messageValue = message.get();
        if (messageValue < currentValue) {
          currentValue = messageValue;
          changed = true;
        }
      }  // end loop thru messages
     
      if (changed) {
        vertex.setValue(new IntWritable(currentValue));
        sendMessageToAllEdges(vertex.getValue())
      }
      
      vertex.voteToHalt();
     
  }
}
