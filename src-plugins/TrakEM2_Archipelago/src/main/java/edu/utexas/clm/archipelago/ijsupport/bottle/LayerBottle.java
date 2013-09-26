package edu.utexas.clm.archipelago.ijsupport.bottle;

import edu.utexas.clm.archipelago.FijiArchipelago;
import edu.utexas.clm.archipelago.network.translation.Bottle;
import ini.trakem2.ControlWindow;
import ini.trakem2.Project;
import ini.trakem2.display.Layer;
import ini.trakem2.persistence.FSLoader;

import java.io.File;

/**
 *
 */
public class LayerBottle implements Bottle<Layer>
{

    //TODO: detect if the project has been opened at all, yet. Otherwise, we get bad breakages.
    private static Project lastProject = null;
    private static File lastFile = null;

    private final double z;
    private final File file;

    public LayerBottle(final Layer l)
    {
        final FSLoader loader = (FSLoader)l.getProject().getLoader();
        file = new File(loader.getProjectXMLPath());
        z = l.getZ();
    }

    public Layer unBottle()
    {
        final Project p;
        final Layer out;

        if (lastProject == null || lastFile == null ||
                !lastFile.getAbsoluteFile().equals(file.getAbsoluteFile()))
        {
            ControlWindow.setGUIEnabled(false);
            p = Project.openFSProject(file.getAbsolutePath(), false);
            lastProject = p;
            lastFile = file;
        }
        else
        {
            p = lastProject;
        }

        return p.getRootLayerSet().getLayer(z);
    }
}
