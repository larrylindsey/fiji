package edu.utexas.clm.archipelago.ijsupport.bottle;

import edu.utexas.clm.archipelago.ijsupport.TrakEM2Archipelago;
import edu.utexas.clm.archipelago.network.translation.Bottle;
import ini.trakem2.Project;
import ini.trakem2.display.Layer;

import java.io.File;

/**
 *
 */
public class LayerBottle implements Bottle<Layer>
{
    private final double z;
    private final File file;

    public LayerBottle(final Layer l)
    {
        file = TrakEM2Archipelago.getFile(l.getProject());
        z = l.getZ();
    }

    public Layer unBottle()
    {
        final Project p = TrakEM2Archipelago.getProject(file);
        return p.getRootLayerSet().getLayer(z);
    }
}
