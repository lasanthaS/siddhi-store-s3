package io.siddhi.extension.store.s3;

import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import org.osgi.framework.BundleContext;
import org.osgi.service.component.annotations.Activate;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.Deactivate;

@Component(immediate = true)
public class ServiceComponent {

    @Activate
    protected void activate(BundleContext bundleContext) {
        System.out.println(">>>>>>>>>> Activated");

        AmazonS3 client = AmazonS3ClientBuilder.standard()
                .withRegion(Regions.DEFAULT_REGION)
                .build();

        boolean test = client.doesBucketExistV2("test");
        System.out.println(">>>>>>>>>>>>>>>>>> test: " + test);
    }

    @Deactivate
    protected void deactivate(BundleContext bundleContext) {
        System.out.println(">>>>>>>>>> Deactivated");
    }
}
