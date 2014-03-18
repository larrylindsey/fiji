package edu.utexas.clm.fixmontage;

import ij.IJ;
import ij.ImageJ;
import ij.Prefs;
import ij.gui.GenericDialog;
import ij.io.OpenDialog;
import ij.plugin.PlugIn;
import ini.trakem2.Project;

import java.awt.*;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.io.File;

/**
 * A PlugIn to fix montage boo-boos caused by using Photoshop or some other non-scientific
 * image-processing software.
 */
public class Fix_Montage implements PlugIn, ActionListener
{
    final static String PREF_ROOT = "FixMontage";
    final static String[] PREF_KEYS = {
            PREF_ROOT + ".traces",
            PREF_ROOT + ".alignment",
            PREF_ROOT + ".rectification",
            PREF_ROOT + ".montage"};

    final Button bTraces, bAlignment, bRectify, bMontage;
    final Button[] buttonArray;
    final GenericDialog gd;

    public Fix_Montage()
    {
        final Panel panelTraces, panelAlignment, panelRectify, panelMontage;

        bTraces = new Button("Select Traces Project...");
        bAlignment = new Button("Select Alignment Project...");
        bRectify = new Button("Select Rectification Project...");
        bMontage = new Button("Select Final Montage Project...");
        gd = new GenericDialog("Defrack photomontage");
        buttonArray = new Button[]{bTraces, bAlignment, bRectify, bMontage};

        panelTraces = new Panel();
        panelAlignment = new Panel();
        panelRectify = new Panel();
        panelMontage = new Panel();

        panelTraces.add(bTraces);
        panelAlignment.add(bAlignment);
        panelRectify.add(bRectify);
        panelMontage.add(bMontage);

        gd.addStringField("Traces Project", Prefs.get(PREF_KEYS[0], ""), 128);
        gd.addPanel(panelTraces);
        gd.addStringField("Alignment Project", Prefs.get(PREF_KEYS[1], ""), 128);
        gd.addPanel(panelAlignment);
        gd.addStringField("Rectification Project", Prefs.get(PREF_KEYS[2], ""), 128);
        gd.addPanel(panelRectify);
        gd.addStringField("Final Montage Project", Prefs.get(PREF_KEYS[3], ""), 128);
        gd.addPanel(panelMontage);

        for (final Button b : buttonArray)
        {
            b.addActionListener(this);
        }
    }

    private boolean check(String path)
    {
        File f = new File(path);
        return f.exists();
    }

    public void run(String s)
    {
        final Project pTraces, pAlignment, pRectify, pMontage;
        final String paths[] = new String[4];
        boolean ok = true;

        gd.showDialog();

        if (!gd.wasOKed())
        {
            return;
        }

        for (int i = 0; i < 4; ++i)
        {
            paths[i] = gd.getNextString();

            if (check(paths[i]))
            {
                Prefs.set(PREF_KEYS[i], paths[i]);
                Prefs.savePreferences();
            }
            else
            {
                IJ.error("Could not find file: " + paths[i]);
                ok = false;
            }
        }

        Prefs.savePreferences();

        if (!ok)
        {
            return;
        }



        pTraces = Project.openFSProject(paths[0]);
        pAlignment = Project.openFSProject(paths[1]);
        pRectify = Project.openFSProject(paths[2]);
        pMontage = Project.openFSProject(paths[3]);

        final FixMontage fix = new FixMontage();
        fix.setAlignmentProject(pAlignment);
        fix.setTracesProject(pTraces);
        fix.setRectifyProject(pRectify);
        fix.setMontageProject(pMontage);

        fix.fixProjects();
    }

    public void actionPerformed(ActionEvent e)
    {
        Object o = e.getSource();
        for (int i = 0; i < buttonArray.length; ++i)
        {
            if (buttonArray[i] == o)
            {
                OpenDialog od = new OpenDialog("Select Project File");
                TextField tf = (TextField)gd.getStringFields().get(i);
                String path = od.getPath();
                if (path != null && !path.isEmpty())
                {
                    tf.setText(od.getPath());
                }
                return;
            }
        }

        IJ.log("aaah! didn't find the source!");

    }
}
