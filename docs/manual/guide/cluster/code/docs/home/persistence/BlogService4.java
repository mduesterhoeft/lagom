package docs.home.persistence;

import akka.NotUsed;
import com.lightbend.lagom.javadsl.api.ServiceCall;

import akka.stream.javadsl.Source;

import com.lightbend.lagom.javadsl.api.*;
import com.lightbend.lagom.javadsl.api.transport.*;
import static com.lightbend.lagom.javadsl.api.Service.*;

public interface BlogService4 extends Service {

  ServiceCall<NotUsed, NotUsed, Source<PostPublished, ?>> getNewPosts();

  @Override
  default Descriptor descriptor() {
    return named("/blogservice").with(
      restCall(Method.GET, "/blogs", getNewPosts())
    );
  }
}
